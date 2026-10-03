"""Wire protocol: turning a TCP byte stream into discrete messages.

TCP hands you an ordered stream of bytes with no notion of where one message
ends and the next begins. A single ``read()`` may return half a message or
three messages glued together. Every network protocol has to solve this, and
the two classic answers are delimiters (HTTP headers end at a blank line) and
length prefixes (say up front how many bytes follow).

openwire uses a length prefix because it is the simplest scheme to get right
and it lets the same connection carry arbitrary binary data later::

    +-------------------+-------------------------------+
    | 4 bytes           | N bytes                       |
    | N as big-endian   | one UTF-8 encoded JSON object |
    +-------------------+-------------------------------+

Every message is a JSON object with a string ``"type"`` field. The original
Napster protocol worked the same way in spirit: a 2-byte length, a 2-byte
numeric message type, then a space-separated ASCII payload. JSON costs a few
more bytes per message but is self-describing, which matters more when the
goal is to read the traffic and understand it.
"""

from __future__ import annotations

import asyncio
import json
import re
import struct
from typing import Any

PROTOCOL_VERSION = 1

#: Upper bound on a single control frame. Without a cap, a hostile peer could
#: announce a 4 GiB frame and make us allocate memory for it.
MAX_FRAME = 1024 * 1024

#: Longest file name that may be registered. Peers truncate longer names
#: before sending; the tracker rejects anything longer.
MAX_NAME_LENGTH = 512

#: Largest file size either side accepts: the biggest integer JSON carries exactly.
MAX_FILE_SIZE = 1 << 53

_HEADER = struct.Struct(">I")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")

Message = dict[str, Any]


class ProtocolError(Exception):
    """The other side sent something that does not follow the protocol."""


class FrameTooLarge(ProtocolError):
    """A frame header announced more bytes than we are willing to read."""


class ConnectionClosed(ProtocolError):
    """The connection ended before a complete message arrived."""


def encode(message: Message) -> bytes:
    """Serialise one message into a length-prefixed frame."""
    body = json.dumps(message, separators=(",", ":")).encode("utf-8")
    if len(body) > MAX_FRAME:
        raise FrameTooLarge(f"message is {len(body)} bytes; the limit is {MAX_FRAME}")
    return _HEADER.pack(len(body)) + body


def decode_body(body: bytes) -> Message:
    """Parse a frame body, insisting on a JSON object with a string 'type'."""
    try:
        message = json.loads(body)
    except (ValueError, RecursionError) as exc:  # thousands of nested brackets blow the parser's stack
        raise ProtocolError("frame body is not valid JSON") from exc
    if not isinstance(message, dict) or not isinstance(message.get("type"), str):
        raise ProtocolError("message must be a JSON object with a string 'type'")
    return message


async def read_message(reader: asyncio.StreamReader, *, timeout: float | None = None) -> Message:
    """Read exactly one framed message from the stream."""
    header = await _read_exactly(reader, _HEADER.size, timeout)
    (length,) = _HEADER.unpack(header)
    if length > MAX_FRAME:
        raise FrameTooLarge(f"peer announced a {length}-byte frame; the limit is {MAX_FRAME}")
    body = await _read_exactly(reader, length, timeout)
    return decode_body(body)


async def write_message(writer: asyncio.StreamWriter, message: Message) -> None:
    """Frame and send one message, waiting until it has left our buffer."""
    writer.write(encode(message))
    await writer.drain()


async def _read_exactly(reader: asyncio.StreamReader, n: int, timeout: float | None) -> bytes:
    try:
        if timeout is None:
            return await reader.readexactly(n)
        return await asyncio.wait_for(reader.readexactly(n), timeout)
    except asyncio.IncompleteReadError as exc:
        raise ConnectionClosed("connection closed mid-message") from exc


def error(code: str, message: str) -> Message:
    """Build the standard error reply."""
    return {"type": "error", "code": code, "message": message}


def normalize_sha256(value: Any) -> str | None:
    """Return the lower-case hex digest if ``value`` looks like a SHA-256, else None."""
    if not isinstance(value, str):
        return None
    value = value.lower()
    return value if _SHA256_RE.match(value) else None


def describe(exc: BaseException) -> str:
    """A human-readable one-liner for an exception, even when str(exc) is empty."""
    return str(exc) or type(exc).__name__


async def close_writer(writer: asyncio.StreamWriter, *, abort: bool = False) -> None:
    """Close a StreamWriter without letting shutdown-time errors escape.

    A graceful close waits for anything still queued to be sent first. When
    the other side has stopped reading, that is a wait that never ends, so
    callers that are giving up on a connection pass ``abort=True`` to drop it
    at once and discard the queue.
    """
    if abort:
        writer.transport.abort()
    else:
        writer.close()
    try:
        await writer.wait_closed()
    except OSError:
        pass
