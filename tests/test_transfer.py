"""Direct peer-to-peer transfers, with no tracker involved."""

import asyncio
from pathlib import Path

import pytest

from openwire.hashing import scan_directory
from openwire.protocol import close_writer, encode, read_message, write_message
from openwire.transfer import (
    PARTIAL_DIRNAME,
    Source,
    TransferError,
    download,
    sanitize_filename,
    serve_file,
    unique_path,
)
from tests.support import free_port, make_file, run


async def start_seeder(index, handler=serve_file):
    """A bare file server. ``handler`` gets (index, reader, writer)."""

    async def on_connect(reader, writer):
        await handler(index, reader, writer)

    server = await asyncio.start_server(on_connect, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    return server, Source(peer_id="seed", nick="seed", host="127.0.0.1", port=port)


async def stop(server):
    server.close()
    await server.wait_closed()


def only(index):
    (shared,) = index.values()
    return shared


def fetch(source, shared, downloads, *, name=None, size=None, progress=None):
    return download(
        source,
        sha256=shared.sha256,
        size=shared.size if size is None else size,
        name=shared.name if name is None else name,
        dest_dir=downloads,
        progress=progress,
    )


def test_download_whole_file(tmp_path):
    share, downloads = tmp_path / "share", tmp_path / "downloads"
    _, data = make_file(share, "Albums/Live/track01.mp3", 300_000)
    index = scan_directory(share)
    shared = only(index)

    async def go():
        server, source = await start_seeder(index)
        seen = []
        try:
            saved = await fetch(source, shared, downloads, progress=lambda done, total: seen.append((done, total)))
        finally:
            await stop(server)
        return saved, seen

    saved, seen = run(go())
    assert saved == downloads / "track01.mp3"  # the folder part of the name is dropped
    assert saved.read_bytes() == data
    assert seen[0] == (0, 300_000) and seen[-1] == (300_000, 300_000)
    assert list((downloads / PARTIAL_DIRNAME).iterdir()) == []


def test_resume_asks_only_for_the_missing_tail(tmp_path):
    share, downloads = tmp_path / "share", tmp_path / "downloads"
    _, data = make_file(share, "big.bin", 200_000)
    index = scan_directory(share)
    shared = only(index)
    partial = downloads / PARTIAL_DIRNAME / f"{shared.sha256}.part"
    partial.parent.mkdir(parents=True)
    partial.write_bytes(data[:120_000])
    offsets = []

    async def recording(index_, reader, writer):
        # Peek at the request, remember its offset, put it back, then serve normally.
        request = await read_message(reader)
        offsets.append(request["offset"])
        reader.feed_data(encode(request))
        await serve_file(index_, reader, writer)

    async def go():
        server, source = await start_seeder(index, recording)
        seen = []
        try:
            saved = await fetch(source, shared, downloads, progress=lambda done, total: seen.append(done))
        finally:
            await stop(server)
        return saved, seen

    saved, seen = run(go())
    assert offsets == [120_000]
    assert seen[0] == 120_000
    assert saved.read_bytes() == data


def test_oversized_partial_is_discarded(tmp_path):
    share, downloads = tmp_path / "share", tmp_path / "downloads"
    _, data = make_file(share, "small.bin", 1_000)
    index = scan_directory(share)
    shared = only(index)
    partial = downloads / PARTIAL_DIRNAME / f"{shared.sha256}.part"
    partial.parent.mkdir(parents=True)
    partial.write_bytes(b"x" * 5_000)

    async def go():
        server, source = await start_seeder(index)
        seen = []
        try:
            saved = await fetch(source, shared, downloads, progress=lambda done, total: seen.append(done))
        finally:
            await stop(server)
        return saved, seen

    saved, seen = run(go())
    assert seen[0] == 0
    assert saved.read_bytes() == data


def test_wrong_bytes_are_rejected_and_discarded(tmp_path):
    share, downloads = tmp_path / "share", tmp_path / "downloads"
    make_file(share, "real.bin", 50_000)
    index = scan_directory(share)
    shared = only(index)

    async def liar(index_, reader, writer):
        request = await read_message(reader)
        await write_message(writer, {"type": "file", "sha256": request["sha256"], "name": shared.name, "size": shared.size})
        writer.write(bytes(shared.size))  # right length, wrong content
        await writer.drain()
        await close_writer(writer)

    async def go():
        server, source = await start_seeder(index, liar)
        try:
            with pytest.raises(TransferError, match="hash mismatch"):
                await fetch(source, shared, downloads)
        finally:
            await stop(server)

    run(go())
    assert list((downloads / PARTIAL_DIRNAME).iterdir()) == []
    assert [p for p in downloads.iterdir() if p.is_file()] == []


def test_short_transfer_keeps_the_partial_and_can_be_resumed(tmp_path):
    share, downloads = tmp_path / "share", tmp_path / "downloads"
    _, data = make_file(share, "movie.bin", 200_000)
    index = scan_directory(share)
    shared = only(index)

    async def quitter(index_, reader, writer):
        await read_message(reader)
        await write_message(writer, {"type": "file", "sha256": shared.sha256, "name": shared.name, "size": shared.size})
        writer.write(data[:100_000])
        await writer.drain()
        await close_writer(writer)

    async def go():
        server, source = await start_seeder(index, quitter)
        try:
            with pytest.raises(TransferError, match="still to come"):
                await fetch(source, shared, downloads)
        finally:
            await stop(server)
        partial = downloads / PARTIAL_DIRNAME / f"{shared.sha256}.part"
        assert partial.stat().st_size == 100_000

        server, source = await start_seeder(index)  # an honest seeder finishes the job
        try:
            saved = await fetch(source, shared, downloads)
        finally:
            await stop(server)
        return saved

    saved = run(go())
    assert saved.read_bytes() == data


def test_seeder_refuses_files_it_does_not_have(tmp_path):
    share, downloads = tmp_path / "share", tmp_path / "downloads"
    make_file(share, "have.bin", 10)
    index = scan_directory(share)

    async def go():
        server, source = await start_seeder(index)
        try:
            with pytest.raises(TransferError, match="not_found"):
                await download(source, sha256="f" * 64, size=10, name="want.bin", dest_dir=downloads)
        finally:
            await stop(server)

    run(go())


def test_size_disagreement_is_rejected(tmp_path):
    share, downloads = tmp_path / "share", tmp_path / "downloads"
    make_file(share, "x.bin", 10)
    index = scan_directory(share)
    shared = only(index)

    async def go():
        server, source = await start_seeder(index)
        try:
            with pytest.raises(TransferError, match="reports size"):
                await fetch(source, shared, downloads, size=11)
        finally:
            await stop(server)

    run(go())


def test_unreachable_source(tmp_path):
    source = Source(peer_id="ghost", nick="ghost", host="127.0.0.1", port=free_port())

    async def go():
        with pytest.raises(TransferError, match="could not connect"):
            await download(source, sha256="a" * 64, size=1, name="x", dest_dir=tmp_path / "dl")

    run(go())


def test_seeder_rejects_malformed_requests(tmp_path):
    share = tmp_path / "share"
    make_file(share, "x.bin", 10)
    index = scan_directory(share)
    shared = only(index)

    async def ask(source, message):
        reader, writer = await asyncio.open_connection(source.host, source.port)
        try:
            await write_message(writer, message)
            return await read_message(reader, timeout=5)
        finally:
            await close_writer(writer)

    async def go():
        server, source = await start_seeder(index)
        try:
            assert (await ask(source, {"type": "hello"}))["code"] == "bad_request"
            assert (await ask(source, {"type": "get", "sha256": "bad"}))["code"] == "bad_request"
            assert (await ask(source, {"type": "get", "sha256": shared.sha256, "offset": -1}))["code"] == "bad_request"
            assert (await ask(source, {"type": "get", "sha256": shared.sha256, "offset": True}))["code"] == "bad_request"
            assert (await ask(source, {"type": "get", "sha256": shared.sha256, "offset": 11}))["code"] == "bad_offset"
            header = await ask(source, {"type": "get", "sha256": shared.sha256.upper(), "offset": 10})
            assert header["type"] == "file" and header["size"] == 10  # offset == size is a legal empty tail
        finally:
            await stop(server)

    run(go())


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("song.mp3", "song.mp3"),
        ("Albums/Live/track01.mp3", "track01.mp3"),
        ("..\\..\\windows\\system32\\evil.dll", "evil.dll"),
        ("../../.bashrc", "bashrc"),  # never a hidden file
        ("..", "FALLBACK"),
        ("...", "FALLBACK"),
        ("", "FALLBACK"),
        ("   ", "FALLBACK"),
        ('what<is>this:"file"?*|', "what_is_this__file____"),
        ("trailing dots and spaces... ", "trailing dots and spaces"),
        ("CON", "_CON"),
        ("nul.txt", "_nul.txt"),
        ("com1.log", "_com1.log"),
        ("console.txt", "console.txt"),
        ("tab\there", "tab_here"),
        ("x" * 300, "x" * 200),
    ],
)
def test_sanitize_filename(raw, expected):
    assert sanitize_filename(raw, fallback="FALLBACK") == expected


def test_unique_path(tmp_path):
    target = tmp_path / "song.mp3"
    assert unique_path(target) == target
    target.write_bytes(b"1")
    assert unique_path(target) == tmp_path / "song (1).mp3"
    (tmp_path / "song (1).mp3").write_bytes(b"2")
    assert unique_path(target) == tmp_path / "song (2).mp3"
    assert unique_path(Path(tmp_path / "noext")) == tmp_path / "noext"
