"""The tracker is driven here with a raw protocol client, not the Peer class,
so these tests pin down the wire protocol itself."""

import asyncio

import pytest

from openwire.protocol import ConnectionClosed, read_message, write_message
from openwire.tracker import MAX_RESULTS, Tracker
from tests.support import run, settle, start_tracker

DIGEST_A = "a" * 64
DIGEST_B = "b" * 64


class Client:
    def __init__(self, reader, writer):
        self.reader, self.writer = reader, writer
        self.welcome = None

    @classmethod
    async def connect(cls, tracker: Tracker, *, port: int = 6699, nick: str | None = None, say_hello: bool = True):
        reader, writer = await asyncio.open_connection("127.0.0.1", tracker.port)
        client = cls(reader, writer)
        if say_hello:
            hello = {"type": "hello", "port": port}
            if nick:
                hello["nick"] = nick
            client.welcome = await client.rpc(hello)
        return client

    async def rpc(self, message):
        await write_message(self.writer, message)
        return await read_message(self.reader, timeout=5)

    async def close(self):
        self.writer.close()
        try:
            await self.writer.wait_closed()
        except OSError:
            pass


def files(*entries):
    return [{"name": name, "size": size, "sha256": digest} for name, size, digest in entries]


def test_hello_welcome_and_leave():
    async def go():
        tracker = await start_tracker()
        try:
            client = await Client.connect(tracker, nick="alice", port=7000)
            welcome = client.welcome
            assert welcome["type"] == "welcome"
            assert welcome["your_host"] == "127.0.0.1"
            assert len(welcome["peer_id"]) == 12
            record = tracker.peers[welcome["peer_id"]]
            assert (record.nick, record.host, record.port) == ("alice", "127.0.0.1", 7000)
            await client.close()
            await settle()
            assert tracker.peers == {}
        finally:
            await tracker.close()

    run(go())


def test_must_say_hello_first():
    async def go():
        tracker = await start_tracker()
        try:
            client = await Client.connect(tracker, say_hello=False)
            reply = await client.rpc({"type": "search", "query": "x"})
            assert (reply["type"], reply["code"]) == ("error", "hello_required")
            with pytest.raises((ConnectionClosed, OSError)):
                await read_message(client.reader, timeout=2)
            await client.close()
        finally:
            await tracker.close()

    run(go())


def test_bad_hello_is_rejected():
    async def go():
        tracker = await start_tracker()
        try:
            client = await Client.connect(tracker, say_hello=False)
            reply = await client.rpc({"type": "hello", "port": 0})
            assert (reply["type"], reply["code"]) == ("error", "bad_request")
            await client.close()
            await settle()
            assert tracker.peers == {}
        finally:
            await tracker.close()

    run(go())


def test_register_search_lookup_and_withdrawal():
    async def go():
        tracker = await start_tracker()
        try:
            alice = await Client.connect(tracker, nick="alice", port=7001)
            bob = await Client.connect(tracker, nick="bob", port=7002)

            reply = await alice.rpc(
                {
                    "type": "register",
                    "files": files(("Music/Blue Train.mp3", 1000, DIGEST_A), ("readme.txt", 10, DIGEST_B)),
                }
            )
            assert reply == {"type": "ok", "count": 2}
            assert tracker.file_count == 2

            # Bob finds Alice's file; matching is case-insensitive and every word must appear.
            reply = await bob.rpc({"type": "search", "query": "blue TRAIN"})
            assert reply["type"] == "results" and reply["total"] == 1
            (hit,) = reply["results"]
            assert (hit["sha256"], hit["name"], hit["size"]) == (DIGEST_A, "Music/Blue Train.mp3", 1000)
            (source,) = hit["sources"]
            assert (source["nick"], source["host"], source["port"]) == ("alice", "127.0.0.1", 7001)
            assert source["peer_id"] == alice.welcome["peer_id"]

            reply = await bob.rpc({"type": "search", "query": "blue banana"})
            assert reply["results"] == []

            # Alice is never offered her own files.
            reply = await alice.rpc({"type": "search", "query": "blue"})
            assert reply["results"] == []

            # Two peers with the same content become one result with two sources.
            await bob.rpc({"type": "register", "files": files(("Blue Train (copy).mp3", 1000, DIGEST_A))})
            carol = await Client.connect(tracker, nick="carol", port=7003)
            reply = await carol.rpc({"type": "search", "query": "blue"})
            (hit,) = reply["results"]
            assert sorted(s["nick"] for s in hit["sources"]) == ["alice", "bob"]

            reply = await carol.rpc({"type": "lookup", "sha256": DIGEST_A.upper()})
            assert reply["type"] == "sources" and reply["sha256"] == DIGEST_A
            assert reply["size"] == 1000 and len(reply["sources"]) == 2

            # Disconnecting withdraws everything, immediately.
            await alice.close()
            await settle()
            reply = await carol.rpc({"type": "lookup", "sha256": DIGEST_A})
            assert [s["nick"] for s in reply["sources"]] == ["bob"]
            reply = await carol.rpc({"type": "search", "query": "readme"})
            assert reply["results"] == []

            await bob.close()
            await carol.close()
        finally:
            await tracker.close()

    run(go())


def test_register_validates_and_keeps_the_old_list_on_failure():
    async def go():
        tracker = await start_tracker()
        try:
            alice = await Client.connect(tracker)
            await alice.rpc({"type": "register", "files": files(("ok.txt", 1, DIGEST_A))})
            for bad in (
                {"type": "register", "files": "nope"},
                {"type": "register", "files": [{"name": "x", "size": 1, "sha256": "short"}]},
                {"type": "register", "files": [{"name": "", "size": 1, "sha256": DIGEST_B}]},
                {"type": "register", "files": [{"name": "x", "size": -1, "sha256": DIGEST_B}]},
                {"type": "register", "files": [{"name": "x", "size": True, "sha256": DIGEST_B}]},
                {"type": "register", "files": ["not an object"]},
            ):
                reply = await alice.rpc(bad)
                assert (reply["type"], reply["code"]) == ("error", "bad_request"), bad
            record = tracker.peers[alice.welcome["peer_id"]]
            assert list(record.files) == [DIGEST_A]
            await alice.close()
        finally:
            await tracker.close()

    run(go())


def test_search_validation_ping_and_unknown_types():
    async def go():
        tracker = await start_tracker()
        try:
            client = await Client.connect(tracker)
            assert await client.rpc({"type": "ping"}) == {"type": "pong"}
            for bad in (
                {"type": "search"},
                {"type": "search", "query": "   "},
                {"type": "search", "query": "x" * 201},
            ):
                reply = await client.rpc(bad)
                assert (reply["type"], reply["code"]) == ("error", "bad_request"), bad
            reply = await client.rpc({"type": "lookup", "sha256": "nope"})
            assert reply["code"] == "bad_request"
            reply = await client.rpc({"type": "dance"})
            assert (reply["type"], reply["code"]) == ("error", "unknown_type")
            reply = await client.rpc({"type": "hello", "port": 1})
            assert reply["code"] == "bad_request"
            await client.close()
        finally:
            await tracker.close()

    run(go())


def test_results_are_capped_and_ranked_by_source_count():
    async def go():
        tracker = await start_tracker()
        try:
            alice = await Client.connect(tracker, nick="alice")
            bob = await Client.connect(tracker, nick="bob")
            carol = await Client.connect(tracker, nick="carol")
            many = [(f"song {i:03d}.mp3", 100, f"{i:064x}") for i in range(MAX_RESULTS + 10)]
            await alice.rpc({"type": "register", "files": files(*many)})
            await bob.rpc({"type": "register", "files": files(many[59])})  # bob also has song 059

            reply = await carol.rpc({"type": "search", "query": "song"})
            assert reply["total"] == MAX_RESULTS + 10
            assert len(reply["results"]) == MAX_RESULTS
            assert reply["results"][0]["name"] == "song 059.mp3"
            assert len(reply["results"][0]["sources"]) == 2
            for client in (alice, bob, carol):
                await client.close()
        finally:
            await tracker.close()

    run(go())


def test_bye_leaves_cleanly():
    async def go():
        tracker = await start_tracker()
        try:
            client = await Client.connect(tracker)
            await client.rpc({"type": "register", "files": files(("a", 1, DIGEST_A))})
            await write_message(client.writer, {"type": "bye"})
            await settle()
            assert tracker.peers == {}
            await client.close()
        finally:
            await tracker.close()

    run(go())


def test_closing_the_tracker_drops_live_peers():
    async def go():
        tracker = await start_tracker()
        client = await Client.connect(tracker)
        await tracker.close()
        with pytest.raises((ConnectionClosed, OSError)):
            await read_message(client.reader, timeout=2)
        await settle()
        assert tracker.peers == {}
        await client.close()

    run(go())
