import asyncio
import json
import struct

import pytest

from openwire.protocol import (
    MAX_FRAME,
    ConnectionClosed,
    FrameTooLarge,
    ProtocolError,
    encode,
    normalize_sha256,
    read_message,
)
from tests.support import run


def feed(*chunks: bytes, eof: bool = True) -> asyncio.StreamReader:
    """A StreamReader pre-loaded with bytes. Call from inside a running loop."""
    reader = asyncio.StreamReader()
    for chunk in chunks:
        reader.feed_data(chunk)
    if eof:
        reader.feed_eof()
    return reader


def test_encode_is_length_prefixed_json():
    message = {"type": "hello", "port": 6699, "nick": "ćevapi"}
    frame = encode(message)
    (length,) = struct.unpack(">I", frame[:4])
    assert length == len(frame) - 4
    assert json.loads(frame[4:].decode("utf-8")) == message


def test_roundtrip():
    message = {"type": "search", "query": "blue train"}

    async def go():
        return await read_message(feed(encode(message)))

    assert run(go()) == message


def test_two_messages_back_to_back():
    async def go():
        reader = feed(encode({"type": "a"}) + encode({"type": "b"}))
        return await read_message(reader), await read_message(reader)

    assert run(go()) == ({"type": "a"}, {"type": "b"})


def test_message_split_across_many_reads():
    frame = encode({"type": "hello", "port": 1})

    async def go():
        reader = asyncio.StreamReader()

        async def trickle():
            for byte in frame:
                reader.feed_data(bytes([byte]))
                await asyncio.sleep(0)
            reader.feed_eof()

        asyncio.create_task(trickle())
        return await read_message(reader)

    assert run(go()) == {"type": "hello", "port": 1}


def test_oversized_frame_is_rejected_before_reading_the_body():
    async def go():
        reader = feed(struct.pack(">I", MAX_FRAME + 1), eof=False)
        with pytest.raises(FrameTooLarge):
            await read_message(reader)

    run(go())


def test_oversized_message_cannot_be_encoded():
    with pytest.raises(FrameTooLarge):
        encode({"type": "x", "pad": "a" * MAX_FRAME})


def test_invalid_json_is_a_protocol_error():
    body = b"{not json"

    async def go():
        with pytest.raises(ProtocolError):
            await read_message(feed(struct.pack(">I", len(body)) + body))

    run(go())


@pytest.mark.parametrize("payload", [b"[1,2]", b'"text"', b'{"no_type": 1}', b'{"type": 5}'])
def test_message_must_be_an_object_with_a_string_type(payload):
    async def go():
        with pytest.raises(ProtocolError):
            await read_message(feed(struct.pack(">I", len(payload)) + payload))

    run(go())


def test_truncated_frame_is_connection_closed():
    frame = encode({"type": "hello"})

    async def go():
        with pytest.raises(ConnectionClosed):
            await read_message(feed(frame[:-3]))

    run(go())


def test_read_timeout():
    async def go():
        with pytest.raises(TimeoutError):
            await read_message(feed(eof=False), timeout=0.05)

    run(go())


def test_normalize_sha256():
    assert normalize_sha256("A" * 64) == "a" * 64
    assert normalize_sha256("a" * 63) is None
    assert normalize_sha256("g" * 64) is None
    assert normalize_sha256(123) is None
    assert normalize_sha256(None) is None
