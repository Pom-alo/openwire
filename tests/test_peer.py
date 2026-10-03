"""End to end: a real tracker and real peers on the loopback interface."""

import asyncio
import hashlib

import pytest

from openwire.peer import Peer, Result, TrackerError
from openwire.tracker import Tracker
from openwire.transfer import Source, TransferError
from tests.support import free_port, make_file, run, settle, start_peer, start_tracker


def test_search_and_download_end_to_end(tmp_path):
    async def go():
        tracker = await start_tracker()
        peers = []
        try:
            _, data = make_file(tmp_path / "alice/share", "Mixtapes/summer 99.mp3", 150_000)
            alice = await start_peer(tracker, tmp_path / "alice/share", tmp_path / "alice/dl", "alice")
            bob = await start_peer(tracker, tmp_path / "bob/share", tmp_path / "bob/dl", "bob")
            peers += [alice, bob]

            assert alice.peer_id != bob.peer_id
            assert alice.observed_host == "127.0.0.1"
            assert tracker.file_count == 1

            assert await alice.search("summer") == []  # never your own files
            results = await bob.search("SUMMER 99")
            assert len(results) == 1
            result = results[0]
            assert (result.name, result.size) == ("Mixtapes/summer 99.mp3", 150_000)
            assert result.sha256 == hashlib.sha256(data).hexdigest()
            assert [s.nick for s in result.sources] == ["alice"]
            assert result.sources[0].port == alice.port

            saved = await bob.download(result)
            assert saved == tmp_path / "bob/dl/summer 99.mp3"
            assert saved.read_bytes() == data

            # A second download does not overwrite the first copy.
            saved2 = await bob.download(result)
            assert saved2 == tmp_path / "bob/dl/summer 99 (1).mp3"

            found = await bob.lookup(result.sha256)
            assert found is not None and [s.nick for s in found.sources] == ["alice"]
            assert await bob.lookup("0" * 64) is None

            with pytest.raises(TrackerError, match="bad_request"):
                await bob.search("   ")
        finally:
            for peer in peers:
                await peer.close()
            await tracker.close()

    run(go())


def test_rescan_and_withdrawal_on_leave(tmp_path):
    async def go():
        tracker = await start_tracker()
        peers = []
        try:
            alice = await start_peer(tracker, tmp_path / "alice/share", tmp_path / "alice/dl", "alice")
            bob = await start_peer(tracker, tmp_path / "bob/share", tmp_path / "bob/dl", "bob")
            peers += [alice, bob]
            assert await bob.search("late") == []

            make_file(tmp_path / "alice/share", "late arrival.txt", 10)
            assert await alice.rescan() == 1
            assert len(await bob.search("late")) == 1

            await alice.close()
            peers.remove(alice)
            await settle()
            assert await bob.search("late") == []
            assert list(tracker.peers) == [bob.peer_id]
        finally:
            for peer in peers:
                await peer.close()
            await tracker.close()

    run(go())


def test_download_fails_over_to_the_next_source(tmp_path):
    async def go():
        tracker = await start_tracker()
        peers = []
        try:
            _, data = make_file(tmp_path / "alice/share", "x.bin", 20_000)
            alice = await start_peer(tracker, tmp_path / "alice/share", tmp_path / "alice/dl", "alice")
            bob = await start_peer(tracker, tmp_path / "bob/share", tmp_path / "bob/dl", "bob")
            peers += [alice, bob]
            (result,) = await bob.search("x.bin")

            ghost = Source(peer_id="ghost", nick="ghost", host="127.0.0.1", port=free_port())
            me = Source(peer_id=bob.peer_id, nick="bob", host="127.0.0.1", port=bob.port)
            rigged = Result(sha256=result.sha256, name=result.name, size=result.size, sources=[me, ghost, *result.sources])
            saved = await bob.download(rigged)
            assert saved.read_bytes() == data

            with pytest.raises(TransferError, match="every source failed"):
                await bob.download(Result(sha256=result.sha256, name=result.name, size=result.size, sources=[ghost]))
            with pytest.raises(TransferError, match="no other peer"):
                await bob.download(Result(sha256=result.sha256, name=result.name, size=result.size, sources=[me]))
        finally:
            for peer in peers:
                await peer.close()
            await tracker.close()

    run(go())


def test_unreachable_tracker_is_a_clean_error(tmp_path):
    async def go():
        peer = Peer(
            tracker=("127.0.0.1", free_port()),
            share_dir=tmp_path,
            download_dir=tmp_path / "dl",
            host="127.0.0.1",
            port=0,
        )
        with pytest.raises(TrackerError, match="cannot reach tracker"):
            await peer.start()
        await peer.close()

    run(go())


def test_peer_reconnects_and_reregisters_after_tracker_restart(tmp_path):
    async def go():
        tracker = await start_tracker()
        port = tracker.port
        make_file(tmp_path / "alice/share", "x.bin", 10)
        alice = await start_peer(
            tracker, tmp_path / "alice/share", tmp_path / "alice/dl", "alice",
            heartbeat_interval=0.1, reconnect_delay=0.1,
        )
        try:
            first_id = alice.peer_id
            await tracker.close()  # the "crash"; live connections are dropped too

            replacement = Tracker("127.0.0.1", port)
            for _ in range(20):
                try:
                    await replacement.start()
                    break
                except OSError:
                    await asyncio.sleep(0.25)
            else:
                pytest.skip("could not rebind the tracker port quickly enough")

            try:
                for _ in range(100):
                    if replacement.file_count == 1:
                        break
                    await asyncio.sleep(0.1)
                assert replacement.file_count == 1
                assert alice.connected
                assert alice.peer_id != first_id  # a fresh hello gets a fresh id
            finally:
                await alice.close()
                await replacement.close()
        except BaseException:
            await alice.close()
            raise

    run(go())
