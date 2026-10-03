"""Hardening against size limits, malformed replies and hostile names.

Each of these tests failed on the first version of Phase 1 and documents a
defect found in the debug sweep."""

import asyncio
import io
import json
import os
import struct
import threading
from pathlib import Path

import pytest

from openwire.cli import build_parser, main, parse_hostport
from openwire.hashing import SharedFile, scan_directory
from openwire.peer import Peer, TrackerError, _paginate
from openwire.protocol import MAX_NAME_LENGTH, ConnectionClosed, ProtocolError, read_message, write_message
from openwire.shell import Shell
from openwire.tracker import MAX_LOOKUP_SOURCES, MAX_SOURCES_PER_RESULT, Tracker
from openwire.transfer import Source, TransferError, download, serve_file
from tests.support import free_port, make_file, run, start_peer, start_tracker
from tests.test_transfer import only, start_seeder, stop


class Script:
    def __init__(self, *lines):
        self.lines = list(lines)

    async def readline(self, prompt):
        return (self.lines.pop(0) + "\n") if self.lines else None


async def raw_client(tracker, nick, files=()):
    reader, writer = await asyncio.open_connection("127.0.0.1", tracker.port)
    await write_message(writer, {"type": "hello", "port": 7000, "nick": nick})
    welcome = await read_message(reader, timeout=5)
    assert welcome["type"] == "welcome"
    if files:
        await write_message(writer, {"type": "register", "files": list(files)})
        assert (await read_message(reader, timeout=5))["type"] == "ok"
    return reader, writer


async def rpc(reader, writer, message):
    await write_message(writer, message)
    return await read_message(reader, timeout=5)


def synthetic_index(count, name_length=41):
    index = {}
    for i in range(count):
        digest = f"{i:064x}"
        name = f"Albums/Artist/Record/track {i:05d} title".ljust(name_length, "x")
        index[digest] = SharedFile(path=Path("nowhere"), name=name, size=3_456_789, sha256=digest)
    return index


def test_peer_registers_more_files_than_fit_in_one_frame(tmp_path):
    async def go():
        tracker = await start_tracker()
        alice = await start_peer(tracker, tmp_path / "share", tmp_path / "dl", "alice")
        try:
            alice.index = synthetic_index(20_000)
            await alice._register()
            assert tracker.file_count == 20_000
            # Re-registering replaces rather than accumulates.
            alice.index = synthetic_index(3)
            await alice._register()
            assert tracker.file_count == 3
            alice.index = {}
            await alice._register()
            assert tracker.file_count == 0
        finally:
            await alice.close()
            await tracker.close()

    run(go())


def test_register_append_flag_is_validated():
    async def go():
        tracker = await start_tracker()
        try:
            reader, writer = await raw_client(tracker, "alice", [{"name": "a", "size": 1, "sha256": "a" * 64}])
            reply = await rpc(reader, writer, {"type": "register", "files": [], "append": "yes"})
            assert (reply["type"], reply["code"]) == ("error", "bad_request")
            reply = await rpc(reader, writer, {"type": "register", "files": [{"name": "b", "size": 1, "sha256": "b" * 64}], "append": True})
            assert reply == {"type": "ok", "count": 2}
            writer.close()
        finally:
            await tracker.close()

    run(go())


def test_peer_truncates_over_long_names_instead_of_failing(tmp_path):
    async def go():
        tracker = await start_tracker()
        alice = await start_peer(tracker, tmp_path / "share", tmp_path / "dl", "alice")
        try:
            digest = "c" * 64
            alice.index = {digest: SharedFile(path=Path("x"), name="n" * 900, size=1, sha256=digest)}
            await alice._register()
            (record,) = tracker.peers.values()
            assert len(record.files[digest].name) == MAX_NAME_LENGTH
        finally:
            await alice.close()
            await tracker.close()

    run(go())


def test_search_and_lookup_cap_the_sources_they_return():
    digest = "d" * 64

    async def go():
        tracker = await start_tracker()
        clients = []
        try:
            for i in range(MAX_LOOKUP_SOURCES + 5):
                clients.append(await raw_client(tracker, f"p{i}", [{"name": "shared.bin", "size": 1, "sha256": digest}]))
            reader, writer = await raw_client(tracker, "asker")
            clients.append((reader, writer))

            reply = await rpc(reader, writer, {"type": "search", "query": "shared"})
            (hit,) = reply["results"]
            assert len(hit["sources"]) == MAX_SOURCES_PER_RESULT
            assert hit["total_sources"] == MAX_LOOKUP_SOURCES + 5

            reply = await rpc(reader, writer, {"type": "lookup", "sha256": digest})
            assert len(reply["sources"]) == MAX_LOOKUP_SOURCES
            assert reply["total"] == MAX_LOOKUP_SOURCES + 5
        finally:
            for _, writer in clients:
                writer.close()
            await tracker.close()

    run(go())


def test_malformed_tracker_replies_become_tracker_errors(tmp_path):
    async def fake_tracker(reader, writer):
        try:
            while True:
                message = await read_message(reader)
                kind = message["type"]
                if kind == "hello":
                    await write_message(
                        writer, {"type": "welcome", "peer_id": "fake00000000", "your_host": "127.0.0.1\x1b[31m"}
                    )
                elif kind == "register":
                    await write_message(writer, {"type": "ok", "count": 0})
                elif kind == "search":
                    await write_message(writer, {"type": "results", "results": [{"bogus": True}]})
                elif kind == "lookup":
                    await write_message(writer, {"type": "sources", "sources": [{"peer_id": "x"}], "name": None, "size": "big"})
                else:
                    break
        except (ConnectionClosed, OSError):
            pass
        finally:
            writer.close()

    async def go():
        server = await asyncio.start_server(fake_tracker, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        peer = Peer(
            tracker=("127.0.0.1", port), share_dir=tmp_path, download_dir=tmp_path / "dl",
            host="127.0.0.1", port=0, heartbeat_interval=1000,
        )
        await peer.start()
        try:
            with pytest.raises(TrackerError, match="malformed"):
                await peer.search("anything")
            with pytest.raises(TrackerError, match="malformed"):
                await peer.lookup("a" * 64)
            assert peer.connected  # bad data is not a reason to drop the link
            assert peer.observed_host is None  # a host with an escape sequence in it is not a host
        finally:
            await peer.close()
            server.close()
            await server.wait_closed()

    run(go())


def test_shell_survives_unexpected_errors_and_hides_control_characters(tmp_path):
    async def go():
        tracker = await start_tracker()
        evil_name = "innocent\x1b[31m RED \x1b[0m‮song.mp3\n\rnext line"
        _, writer = await raw_client(tracker, "mallory", [{"name": evil_name, "size": 10, "sha256": "e" * 64}])
        bob = await start_peer(tracker, tmp_path / "bob/share", tmp_path / "bob/dl", "bob")
        try:
            out = io.StringIO()
            await Shell(bob, out=out).run(Script("search innocent", "status", "quit"))
            text_ok = out.getvalue()

            async def boom(query):
                raise RuntimeError("kaboom")

            bob.search = boom
            out = io.StringIO()
            script = Script("search x", "status", "quit")
            await Shell(bob, out=out).run(script)
            return text_ok, out.getvalue(), script
        finally:
            writer.close()
            await bob.close()
            await tracker.close()

    text_ok, text_boom, script = run(go())
    assert "innocent" in text_ok
    assert "\x1b" not in text_ok and "‮" not in text_ok
    assert "next line" in text_ok and "\n\r" not in text_ok
    assert "kaboom" in text_boom
    assert "peer id" in text_boom  # the shell kept going after the error
    assert script.lines == []


def test_shell_explains_a_bad_result_number(tmp_path):
    async def go():
        tracker = await start_tracker()
        bob = await start_peer(tracker, tmp_path / "bob/share", tmp_path / "bob/dl", "bob")
        try:
            out = io.StringIO()
            await Shell(bob, out=out).run(Script("get 3", "quit"))
            return out.getvalue()
        finally:
            await bob.close()
            await tracker.close()

    assert "run a search first" in run(go())


def test_download_filesystem_errors_become_transfer_errors(tmp_path):
    blocker = tmp_path / "not-a-dir"
    blocker.write_bytes(b"i am a file where a folder should be")

    async def go():
        with pytest.raises(TransferError, match="download folder"):
            await download(Source("p", "p", "127.0.0.1", 1), sha256="a" * 64, size=1, name="x", dest_dir=blocker)

    run(go())


def test_source_from_dict_rejects_garbage():
    for bad in ({}, {"peer_id": "p"}, {"peer_id": "p", "host": "h", "port": 0}, {"peer_id": "p", "host": "h", "port": True}, {"peer_id": "p", "host": "", "port": 1}, "text"):
        with pytest.raises((TypeError, ValueError)):
            Source.from_dict(bad)
    source = Source.from_dict({"peer_id": "p", "host": "h", "port": 1})
    assert source.nick == "p"


def test_silent_connections_are_dropped_quickly():
    async def go():
        tracker = Tracker("127.0.0.1", 0, hello_timeout=0.3, idle_timeout=60)
        await tracker.start()
        try:
            reader, writer = await asyncio.open_connection("127.0.0.1", tracker.port)
            with pytest.raises((ConnectionClosed, OSError)):
                await read_message(reader, timeout=3)
            writer.close()
        finally:
            await tracker.close()

    run(go())


def test_tracker_strips_nick_whitespace():
    async def go():
        tracker = await start_tracker()
        try:
            reader, writer = await asyncio.open_connection("127.0.0.1", tracker.port)
            welcome = await rpc(reader, writer, {"type": "hello", "port": 7000, "nick": "  alice  "})
            assert tracker.peers[welcome["peer_id"]].nick == "alice"
            writer.close()
        finally:
            await tracker.close()

    run(go())


def test_parse_hostport_accepts_bracketed_ipv6():
    assert parse_hostport("[::1]:9000", 8888) == ("::1", 9000)
    assert parse_hostport("[fe80::1]", 8888) == ("fe80::1", 8888)
    import argparse

    for bad in ("[::1", "[::1]x", "[::1]:abc"):
        with pytest.raises(argparse.ArgumentTypeError):
            parse_hostport(bad, 8888)


# ---------------------------------------------------------------------------
# Second round: the tracker link under stress, and seeders facing bad clients.


async def stalling_tracker(reader, writer):
    """Speaks the protocol except that it never answers a search."""
    try:
        while True:
            message = await read_message(reader)
            kind = message["type"]
            if kind == "hello":
                await write_message(writer, {"type": "welcome", "peer_id": "stall0000000", "your_host": "127.0.0.1"})
            elif kind == "register":
                await write_message(writer, {"type": "ok", "count": 0})
            elif kind == "ping":
                await write_message(writer, {"type": "pong"})
            elif kind == "search":
                pass  # the whole point: leave the peer hanging
            else:
                break
    except (ConnectionClosed, OSError):
        pass
    finally:
        writer.close()


@pytest.mark.parametrize("heartbeat", [0.1, 1000.0], ids=["ping queued behind the stalled request", "no ping due for ages"])
def test_peer_recovers_from_a_request_that_times_out(tmp_path, heartbeat):
    """The request times out while the heartbeat may be waiting on the link lock.
    The keepalive must survive, and the reconnect must not wait for the next heartbeat."""

    async def go():
        server = await asyncio.start_server(stalling_tracker, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        peer = Peer(
            tracker=("127.0.0.1", port), share_dir=tmp_path, download_dir=tmp_path / "dl", host="127.0.0.1", port=0,
            heartbeat_interval=heartbeat, request_timeout=0.4, reconnect_delay=0.1,
        )
        await peer.start()
        try:
            with pytest.raises(TrackerError, match="lost the tracker"):
                await peer.search("anything")
            for _ in range(50):
                if peer.connected:
                    break
                await asyncio.sleep(0.1)
            assert peer.connected
            assert not peer._keepalive_task.done()
            assert await peer._request({"type": "ping"}) == {"type": "pong"}
        finally:
            await peer.close()
            server.close()
            await server.wait_closed()

    run(go())


def test_seeder_says_busy_when_its_upload_slots_are_full(tmp_path):
    async def go():
        tracker = await start_tracker()
        make_file(tmp_path / "share", "x.bin", 10)
        alice = await start_peer(tracker, tmp_path / "share", tmp_path / "dl", "alice", max_uploads=1)
        (shared,) = alice.index.values()
        get = {"type": "get", "sha256": shared.sha256, "offset": 0}
        try:
            # The first connection takes the only slot and says nothing; the seeder waits for its request.
            _, hog = await asyncio.open_connection("127.0.0.1", alice.port)
            await asyncio.sleep(0.1)
            reader, writer = await asyncio.open_connection("127.0.0.1", alice.port)
            await write_message(writer, get)
            reply = await read_message(reader, timeout=5)
            assert (reply["type"], reply["code"]) == ("error", "busy")
            writer.close()
            hog.close()
            await asyncio.sleep(0.2)

            reader, writer = await asyncio.open_connection("127.0.0.1", alice.port)  # the slot is free again
            await write_message(writer, get)
            assert (await read_message(reader, timeout=5))["type"] == "file"
            writer.close()
        finally:
            await alice.close()
            await tracker.close()

    run(go())


def test_seeder_gives_up_on_a_client_that_stops_reading(tmp_path):
    big = tmp_path / "big.bin"
    big.write_bytes(os.urandom(16 * 1024 * 1024))
    digest = "f" * 64
    index = {digest: SharedFile(path=big, name="big.bin", size=big.stat().st_size, sha256=digest)}

    async def go():
        finished = asyncio.Event()

        async def impatient(index_, reader, writer):
            try:
                await serve_file(index_, reader, writer, write_timeout=0.5)
            finally:
                finished.set()

        server, source = await start_seeder(index, impatient)
        reader, writer = await asyncio.open_connection(source.host, source.port)
        try:
            await write_message(writer, {"type": "get", "sha256": digest, "offset": 0})
            assert (await read_message(reader, timeout=5))["type"] == "file"
            # ... and now read nothing more. The seeder must not wait forever.
            await asyncio.wait_for(finished.wait(), 15)
        finally:
            writer.close()
            await stop(server)

    run(go())


def test_download_rejects_a_partial_someone_else_wrote_to(tmp_path):
    share, downloads = tmp_path / "share", tmp_path / "downloads"
    make_file(share, "x.bin", 300_000)
    index = scan_directory(share)
    shared = only(index)
    part = downloads / ".partial" / f"{shared.sha256}.part"
    sabotaged = []

    def sabotage(done, total):
        if done and not sabotaged:  # once the first chunk has landed, someone else appends to the partial
            sabotaged.append(True)
            with part.open("ab") as handle:
                handle.write(b"junk" * 10)

    async def go():
        server, source = await start_seeder(index)
        try:
            with pytest.raises(TransferError, match="another download may be writing"):
                await download(source, sha256=shared.sha256, size=shared.size, name=shared.name, dest_dir=downloads, progress=sabotage)
        finally:
            await stop(server)

    run(go())
    assert sabotaged
    assert not part.exists()
    assert [p for p in downloads.iterdir() if p.is_file()] == []


def test_tracker_turns_away_connections_past_its_limit():
    async def go():
        tracker = Tracker("127.0.0.1", 0, max_connections=2)
        await tracker.start()
        try:
            first = await raw_client(tracker, "one")
            second = await raw_client(tracker, "two")
            reader, writer = await asyncio.open_connection("127.0.0.1", tracker.port)
            reply = await read_message(reader, timeout=5)
            assert (reply["type"], reply["code"]) == ("error", "full")
            with pytest.raises((ConnectionClosed, OSError)):
                await read_message(reader, timeout=2)
            writer.close()
            first[1].close()
            await asyncio.sleep(0.2)
            third = await raw_client(tracker, "three")  # a slot freed up
            third[1].close()
            second[1].close()
        finally:
            await tracker.close()

    run(go())


def test_tracker_index_has_a_total_size_limit():
    async def go():
        tracker = Tracker("127.0.0.1", 0, max_total_files=5)
        await tracker.start()
        try:
            files = lambda start, n: [{"name": f"f{i}", "size": 1, "sha256": f"{i:064x}"} for i in range(start, start + n)]
            alice = await raw_client(tracker, "alice", files(0, 3))
            bob = await raw_client(tracker, "bob")
            reply = await rpc(*bob, {"type": "register", "files": files(10, 3)})
            assert reply["code"] == "bad_request" and "full" in reply["message"]
            reply = await rpc(*bob, {"type": "register", "files": files(10, 2)})
            assert reply == {"type": "ok", "count": 2}
            # Replacing your own list only counts the difference.
            reply = await rpc(*alice, {"type": "register", "files": files(20, 3)})
            assert reply == {"type": "ok", "count": 3}
            alice[1].close()
            bob[1].close()
        finally:
            await tracker.close()

    run(go())


def test_result_size_is_bounded():
    from openwire.peer import Result
    from openwire.protocol import MAX_FILE_SIZE

    Result.from_dict({"sha256": "a" * 64, "name": "x", "size": MAX_FILE_SIZE, "sources": []})
    with pytest.raises(ValueError):
        Result.from_dict({"sha256": "a" * 64, "name": "x", "size": MAX_FILE_SIZE + 1, "sources": []})
    with pytest.raises(ValueError):
        Result.from_dict({"sha256": "a" * 64, "name": "x", "size": 10 ** 400, "sources": []})


def test_sanitize_filename_drops_bidi_and_format_characters():
    from openwire.transfer import sanitize_filename

    assert sanitize_filename("photo‮gpj.exe", fallback="f") == "photo_gpj.exe"
    assert sanitize_filename("zero​width.txt", fallback="f") == "zero_width.txt"


def test_deeply_nested_json_is_a_protocol_error():
    body = b"[" * 20_000 + b"]" * 20_000

    async def go():
        reader = asyncio.StreamReader()
        reader.feed_data(struct.pack(">I", len(body)) + body)
        reader.feed_eof()
        with pytest.raises(ProtocolError):
            await read_message(reader)

    run(go())


# ---------------------------------------------------------------------------
# Third round: the command line itself, and loose ends from the docs review.


def test_port_arguments_are_range_checked():
    parser = build_parser()
    for bad in ("70000", "-1", "abc"):
        for command in ("peer", "tracker"):
            with pytest.raises(SystemExit):
                parser.parse_args([command, "--port", bad])
    assert parser.parse_args(["peer", "--port", "0"]).port == 0


def tracker_in_a_thread():
    """A real tracker on its own event loop, so main() can be driven with asyncio.run()."""
    ready = threading.Event()
    box: dict = {}

    def target():
        async def serve():
            tracker = Tracker("127.0.0.1", 0)
            await tracker.start()
            box["port"] = tracker.port
            box["loop"] = asyncio.get_running_loop()
            box["stop"] = asyncio.Event()
            ready.set()
            await box["stop"].wait()
            await tracker.close()

        asyncio.run(serve())

    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    assert ready.wait(5)

    def stop():
        box["loop"].call_soon_threadsafe(box["stop"].set)
        thread.join(5)

    return box["port"], stop


def test_cli_exit_codes_and_messages(tmp_path, capsys):
    share = tmp_path / "share"
    share.mkdir()

    assert main(["peer", "--share", str(tmp_path / "nope"), "--tracker", "127.0.0.1:1", "--port", "0"]) == 1
    assert "does not exist" in capsys.readouterr().err

    assert main(["peer", "--share", str(share), "--tracker", "127.0.0.1:notaport", "--port", "0"]) == 2
    assert "--tracker" in capsys.readouterr().err

    assert main(["peer", "--share", str(share), "--tracker", f"127.0.0.1:{free_port()}", "--port", "0"]) == 1
    assert "Is a tracker running?" in capsys.readouterr().err

    port, stop = tracker_in_a_thread()
    try:
        # The tracker rejects a 33-character nick: an error, but not a "tracker down" hint.
        assert main(["peer", "--share", str(share), "--tracker", f"127.0.0.1:{port}", "--port", "0", "--nick", "n" * 33]) == 1
        err = capsys.readouterr().err
        assert "turned us away" in err and "Is a tracker running?" not in err

        # A healthy run: pytest's stdin yields nothing, so the shell exits at once.
        assert main(["peer", "--share", str(share), "--tracker", f"127.0.0.1:{port}", "--port", "0", "--nick", "cli"]) == 0
        out = capsys.readouterr().out
        assert "openwire" in out and "Type 'help'" in out
    finally:
        stop()


def test_paginate_respects_the_byte_budget():
    assert _paginate([], 100) == [[]]
    entries = [{"name": f"file {i:03d}", "size": 1, "sha256": f"{i:064x}"} for i in range(50)]
    one = len(json.dumps(entries[0], separators=(",", ":"))) + 1
    pages = _paginate(entries, one * 7)
    assert [entry for page in pages for entry in page] == entries
    assert len(pages) == 8 and all(len(page) <= 7 for page in pages)
    assert _paginate(entries[:1], 1) == [entries[:1]]  # an entry over budget still gets a page of its own


def test_shell_accepts_tabs_and_shows_total_sources(tmp_path):
    digest = "a" * 64

    async def go():
        tracker = await start_tracker()
        clients = [
            await raw_client(tracker, f"p{i}", [{"name": "popular.mp3", "size": 5, "sha256": digest}])
            for i in range(MAX_SOURCES_PER_RESULT + 3)
        ]
        bob = await start_peer(tracker, tmp_path / "share", tmp_path / "dl", "bob")
        try:
            out = io.StringIO()
            await Shell(bob, out=out).run(Script("search\tpopular", "quit"))
            return out.getvalue()
        finally:
            for _, writer in clients:
                writer.close()
            await bob.close()
            await tracker.close()

    text = run(go())
    assert f"{MAX_SOURCES_PER_RESULT + 3:>4}  popular.mp3" in text


def test_hello_host_override_and_null_fields():
    async def go():
        tracker = await start_tracker()
        writers = []
        try:
            reader, writer = await asyncio.open_connection("127.0.0.1", tracker.port)
            writers.append(writer)
            welcome = await rpc(reader, writer, {"type": "hello", "port": 7000, "host": "10.9.8.7", "nick": None})
            assert welcome["your_host"] == "10.9.8.7"
            record = tracker.peers[welcome["peer_id"]]
            assert (record.host, record.nick) == ("10.9.8.7", welcome["peer_id"])

            reader, writer = await asyncio.open_connection("127.0.0.1", tracker.port)
            writers.append(writer)
            welcome = await rpc(reader, writer, {"type": "hello", "port": 7001, "host": None})
            assert welcome["your_host"] == "127.0.0.1"

            reader, writer = await asyncio.open_connection("127.0.0.1", tracker.port)
            writers.append(writer)
            reply = await rpc(reader, writer, {"type": "hello", "port": 7002, "nick": 5})
            assert reply["code"] == "bad_request"
        finally:
            for writer in writers:
                writer.close()
            await tracker.close()

    run(go())


def test_seeder_reports_a_file_that_vanished(tmp_path):
    share, downloads = tmp_path / "share", tmp_path / "dl"
    path, _ = make_file(share, "gone.bin", 100)
    index = scan_directory(share)
    shared = only(index)
    path.unlink()

    async def go():
        server, source = await start_seeder(index)
        try:
            with pytest.raises(TransferError, match="unavailable"):
                await download(source, sha256=shared.sha256, size=shared.size, name=shared.name, dest_dir=downloads)
        finally:
            await stop(server)

    run(go())


def test_bye_as_first_message_closes_quietly():
    async def go():
        tracker = await start_tracker()
        try:
            reader, writer = await asyncio.open_connection("127.0.0.1", tracker.port)
            await write_message(writer, {"type": "bye"})
            with pytest.raises((ConnectionClosed, OSError)):  # EOF, not an error frame
                await read_message(reader, timeout=2)
            writer.close()
        finally:
            await tracker.close()

    run(go())
