"""A peer: shares a folder, serves it to other peers, and uses a tracker to find files.

One :class:`Peer` runs three things concurrently on the asyncio loop:

1. a **file server** on its own port, answering ``get`` requests from other
   peers (see :mod:`openwire.transfer`);
2. a **tracker link**: one long-lived TCP connection over which it registers
   its files, searches, and sends a heartbeat. If the link drops it reconnects
   and re-registers, because the tracker forgets everything about a peer the
   moment its connection closes;
3. whatever the user asks for from the shell: searches and downloads.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from pathlib import Path

from .hashing import SharedFile, scan_directory
from .protocol import (
    PROTOCOL_VERSION,
    ConnectionClosed,
    Message,
    ProtocolError,
    close_writer,
    describe,
    read_message,
    write_message,
)
from .transfer import ProgressCallback, Source, TransferError, download, serve_file

log = logging.getLogger("openwire.peer")

DEFAULT_PORT = 6699  # Napster clients accepted incoming transfers on 6699


class TrackerError(Exception):
    """The tracker rejected a request, or could not be reached."""


@dataclass
class Result:
    """One search hit: a file (by content) and every peer currently offering it."""

    sha256: str
    name: str
    size: int
    sources: list[Source] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: Message) -> "Result":
        return cls(
            sha256=data["sha256"],
            name=str(data["name"]),
            size=int(data["size"]),
            sources=[Source.from_dict(s) for s in data.get("sources", [])],
        )


class Peer:
    def __init__(
        self,
        *,
        tracker: tuple[str, int],
        share_dir: Path,
        download_dir: Path,
        host: str = "127.0.0.1",
        port: int = DEFAULT_PORT,
        nick: str | None = None,
        advertise_host: str | None = None,
        heartbeat_interval: float = 30.0,
        request_timeout: float = 30.0,
        reconnect_delay: float = 2.0,
    ) -> None:
        self.tracker_addr = tracker
        self.share_dir = Path(share_dir)
        self.download_dir = Path(download_dir)
        self.host = host
        self.port = port
        self.nick = nick
        self.advertise_host = advertise_host
        self.heartbeat_interval = heartbeat_interval
        self.request_timeout = request_timeout
        self.reconnect_delay = reconnect_delay

        self.index: dict[str, SharedFile] = {}
        self.peer_id: str | None = None
        self.observed_host: str | None = None  # our address as the tracker sees it
        self.connected = False

        self._server: asyncio.base_events.Server | None = None
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._lock = asyncio.Lock()  # one request in flight on the tracker link at a time
        self._keepalive_task: asyncio.Task | None = None

    # ------------------------------------------------------------ lifecycle --

    async def start(self) -> None:
        # Hashing is CPU and disk bound; keep it off the event loop thread.
        self.index = await asyncio.to_thread(scan_directory, self.share_dir)
        self._server = await asyncio.start_server(self._on_incoming, self.host, self.port)
        self.port = self._server.sockets[0].getsockname()[1]
        log.info("serving %d files on %s:%d", len(self.index), self.host, self.port)
        await self._connect_tracker()
        self._keepalive_task = asyncio.create_task(self._keepalive(), name="openwire-keepalive")

    async def close(self) -> None:
        if self._keepalive_task is not None:
            self._keepalive_task.cancel()
            try:
                await self._keepalive_task
            except asyncio.CancelledError:
                pass
            self._keepalive_task = None
        if self.connected and self._writer is not None:
            try:
                await asyncio.wait_for(write_message(self._writer, {"type": "bye"}), 2.0)
            except (OSError, TimeoutError, ProtocolError):
                pass
        await self._disconnect_tracker()
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

    # ------------------------------------------------------- tracker link --

    async def _connect_tracker(self) -> None:
        host, port = self.tracker_addr
        try:
            self._reader, self._writer = await asyncio.wait_for(
                asyncio.open_connection(host, port), self.request_timeout
            )
        except (OSError, TimeoutError) as exc:
            raise TrackerError(f"cannot reach tracker at {host}:{port} ({describe(exc)})") from exc
        self.connected = True

        hello: Message = {"type": "hello", "version": PROTOCOL_VERSION, "port": self.port}
        if self.nick:
            hello["nick"] = self.nick
        if self.advertise_host:
            hello["host"] = self.advertise_host
        try:
            welcome = await self._request(hello)
            if welcome["type"] != "welcome":
                raise TrackerError(f"unexpected reply to hello: {welcome['type']}")
            self.peer_id = welcome["peer_id"]
            self.observed_host = welcome.get("your_host")
            await self._register()
        except TrackerError:
            await self._disconnect_tracker()
            raise
        log.info("joined tracker %s:%d as %s", host, port, self.peer_id)

    async def _register(self) -> None:
        reply = await self._request({"type": "register", "files": [f.to_meta() for f in self.index.values()]})
        if reply["type"] != "ok":
            raise TrackerError(f"unexpected reply to register: {reply['type']}")

    async def _request(self, message: Message) -> Message:
        """Send one message on the tracker link and return its reply."""
        if not self.connected or self._writer is None or self._reader is None:
            raise TrackerError("not connected to the tracker (reconnecting in the background)")
        try:
            async with self._lock:
                await write_message(self._writer, message)
                reply = await read_message(self._reader, timeout=self.request_timeout)
        except (ConnectionClosed, TimeoutError, OSError) as exc:
            await self._disconnect_tracker()
            raise TrackerError(f"lost the tracker connection ({describe(exc)})") from exc
        except ProtocolError as exc:
            await self._disconnect_tracker()
            raise TrackerError(f"tracker sent something unreadable: {exc}") from exc
        if reply["type"] == "error":
            raise TrackerError(f"{reply.get('code', 'error')}: {reply.get('message', '')}")
        return reply

    async def _disconnect_tracker(self) -> None:
        self.connected = False
        writer, self._writer, self._reader = self._writer, None, None
        if writer is not None:
            await close_writer(writer)

    async def _keepalive(self) -> None:
        """Ping the tracker on a timer; reconnect (and re-register) if the link dies."""
        backoff = self.reconnect_delay
        while True:
            if not self.connected:
                try:
                    await self._connect_tracker()
                    backoff = self.reconnect_delay
                    log.info("reconnected to tracker as %s", self.peer_id)
                except TrackerError as exc:
                    log.warning("tracker reconnect failed: %s; retrying in %.1fs", exc, backoff)
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 30.0)
                continue
            await asyncio.sleep(self.heartbeat_interval)
            if not self.connected:
                continue
            try:
                reply = await self._request({"type": "ping"})
                if reply["type"] != "pong":
                    log.warning("tracker answered ping with %r", reply["type"])
            except TrackerError as exc:
                log.warning("lost tracker connection: %s", exc)

    # ---------------------------------------------------------- user actions --

    async def search(self, query: str) -> list[Result]:
        reply = await self._request({"type": "search", "query": query})
        return [Result.from_dict(r) for r in reply.get("results", [])]

    async def lookup(self, sha256: str) -> Result | None:
        """Every peer currently offering this exact file, or None if nobody is."""
        reply = await self._request({"type": "lookup", "sha256": sha256})
        if not reply.get("sources"):
            return None
        return Result.from_dict(reply)

    async def download(self, result: Result, progress: ProgressCallback | None = None) -> Path:
        """Try each source in turn until one delivers the file intact."""
        failures: list[str] = []
        for source in result.sources:
            if source.peer_id == self.peer_id:
                continue
            try:
                return await download(
                    source,
                    sha256=result.sha256,
                    size=result.size,
                    name=result.name,
                    dest_dir=self.download_dir,
                    progress=progress,
                )
            except TransferError as exc:
                log.warning("%s", exc)
                failures.append(str(exc))
        if not failures:
            raise TransferError("no other peer is sharing that file")
        raise TransferError("every source failed:\n  " + "\n  ".join(failures))

    async def rescan(self) -> int:
        """Re-hash the share folder and tell the tracker about the new list."""
        self.index = await asyncio.to_thread(scan_directory, self.share_dir)
        if self.connected:
            await self._register()
        return len(self.index)

    async def _on_incoming(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await serve_file(self.index, reader, writer)
