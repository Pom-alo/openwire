"""Shared helpers for the test suite: a loopback tracker, loopback peers, random files."""

from __future__ import annotations

import asyncio
import os
import socket
from pathlib import Path

from openwire.peer import Peer
from openwire.tracker import Tracker


def run(coro):
    """Run a coroutine to completion on a fresh event loop."""
    return asyncio.run(coro)


def make_file(directory: Path, name: str, size: int) -> tuple[Path, bytes]:
    """Create a file of random bytes; return (path, contents)."""
    data = os.urandom(size)
    path = directory / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return path, data


def free_port() -> int:
    """A port nothing is listening on right now."""
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


async def start_tracker() -> Tracker:
    tracker = Tracker("127.0.0.1", 0)
    await tracker.start()
    return tracker


async def start_peer(tracker: Tracker, share: Path, downloads: Path, nick: str, **overrides) -> Peer:
    share.mkdir(parents=True, exist_ok=True)
    options = dict(
        tracker=("127.0.0.1", tracker.port),
        share_dir=share,
        download_dir=downloads,
        host="127.0.0.1",
        port=0,
        nick=nick,
        heartbeat_interval=1000.0,  # keep the heartbeat out of the way unless a test wants it
    )
    options.update(overrides)
    peer = Peer(**options)
    await peer.start()
    return peer


async def settle(delay: float = 0.2) -> None:
    """Give the other side's handler a moment to notice a disconnect."""
    await asyncio.sleep(delay)
