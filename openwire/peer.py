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
import json
import logging
from dataclasses import dataclass, field
from pathlib import Path

from . import protocol
from .hashing import SharedFile, scan_directory_async
from .protocol import (
    MAX_FILE_SIZE,
    MAX_NAME_LENGTH,
    PROTOCOL_VERSION,
    ConnectionClosed,
    FrameTooLarge,
    Message,
    ProtocolError,
    close_writer,
    describe,
    encode,
    normalize_sha256,
    read_message,
    write_message,
)
from .transfer import ProgressCallback, Source, TransferError, download, serve_file

log = logging.getLogger("openwire.peer")

DEFAULT_PORT = 6699  # Napster clients accepted incoming transfers on 6699

#: A share folder with thousands of files does not fit in one 1 MiB frame, so
#: the file list goes to the tracker in pages of at most this many bytes.
REGISTER_PAGE_BYTES = 512 * 1024

#: How many files this peer will send at the same time before telling new
#: downloaders it is busy.
DEFAULT_MAX_UPLOADS = 32


class TrackerError(Exception):
    """The tracker rejected a request, could not be reached, or sent nonsense."""


class TrackerUnreachable(TrackerError):
    """No TCP connection to the tracker could be made at all."""


@dataclass
class Result:
    """One search hit: a file (by content) and the peers currently offering it.

    ``sources`` is what the tracker listed (it caps the list); ``total_sources``
    is how many peers have the file in all.
    """

    sha256: str
    name: str
    size: int
    sources: list[Source] = field(default_factory=list)
    total_sources: int = 0

    def __post_init__(self) -> None:
        self.total_sources = max(self.total_sources, len(self.sources))

    @classmethod
    def from_dict(cls, data: Message) -> "Result":
        """Build from a tracker reply, refusing anything malformed."""
        digest = normalize_sha256(data.get("sha256"))
        name, size, sources = data.get("name"), data.get("size"), data.get("sources", [])
        if digest is None or not isinstance(name, str) or not isinstance(sources, list):
            raise ValueError("malformed result")
        if isinstance(size, bool) or not isinstance(size, int) or not 0 <= size <= MAX_FILE_SIZE:
            raise ValueError("malformed result size")
        total = data.get("total_sources", data.get("total", 0))
        if isinstance(total, bool) or not isinstance(total, int):
            total = 0
        return cls(
            sha256=digest, name=name, size=size, sources=[Source.from_dict(s) for s in sources], total_sources=total
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
        max_uploads: int = DEFAULT_MAX_UPLOADS,
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
        self.max_uploads = max_uploads

        self.index: dict[str, SharedFile] = {}
        self.peer_id: str | None = None
        self.observed_host: str | None = None  # our address as the tracker sees it
        self.connected = False

        self._server: asyncio.base_events.Server | None = None
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        # The tracker link is strict request/response, so every use of it,
        # including connecting and the hello/register handshake, holds this lock.
        self._lock = asyncio.Lock()
        self._link_lost = asyncio.Event()  # wakes the keepalive the moment the link dies
        self._download_locks: dict[str, asyncio.Lock] = {}  # one download per file at a time
        self._upload_slots = asyncio.Semaphore(max_uploads)
        self._incoming: set[asyncio.StreamWriter] = set()  # transfers in progress, so close() can cut them
        self._keepalive_task: asyncio.Task | None = None

    # ------------------------------------------------------------ lifecycle --

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._on_incoming, self.host, self.port)
        self.port = self._server.sockets[0].getsockname()[1]
        # Join before hashing, with nothing registered yet, so a dead tracker is
        # reported at once rather than after a long scan of a big share folder.
        await self._connect_tracker()
        self.index = await scan_directory_async(self.share_dir)
        log.info("serving %d files on %s:%d", len(self.index), self.host, self.port)
        try:
            await self._register()
        except TrackerError as exc:
            log.warning("could not register after indexing (%s); the reconnect will", exc)
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
        for writer in list(self._incoming):  # before wait_closed(), which on 3.12+ waits for handlers
            writer.transport.abort()
        self._incoming.clear()
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

    # ------------------------------------------------------- tracker link --

    async def _connect_tracker(self) -> None:
        """Open the link, say hello, register, and only then publish it as live."""
        host, port = self.tracker_addr
        async with self._lock:
            try:
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(host, port), self.request_timeout
                )
            except (OSError, TimeoutError) as exc:
                raise TrackerUnreachable(f"cannot reach tracker at {host}:{port} ({describe(exc)})") from exc

            hello: Message = {"type": "hello", "version": PROTOCOL_VERSION, "port": self.port}
            if self.nick:
                hello["nick"] = self.nick
            if self.advertise_host:
                hello["host"] = self.advertise_host
            try:
                welcome = await self._exchange(reader, writer, hello)
                peer_id = welcome.get("peer_id")
                if welcome["type"] != "welcome" or not isinstance(peer_id, str) or not peer_id:
                    raise TrackerError(f"unexpected reply to hello: {welcome['type']}")
                your_host = welcome.get("your_host")
                await self._send_registration(reader, writer)
            except TrackerError:
                await close_writer(writer)
                raise

            self._reader, self._writer = reader, writer
            self.peer_id = peer_id
            # Shown to the user, so it must at least look like an address.
            plausible = isinstance(your_host, str) and 0 < len(your_host) <= 253 and your_host.isprintable()
            self.observed_host = your_host if plausible else None
            self.connected = True
            self._link_lost.clear()
        log.info("joined tracker %s:%d as %s", host, port, self.peer_id)

    async def _register(self) -> None:
        """Re-send the whole file list on the live link."""
        async with self._lock:
            reader, writer = self._live_link()
            await self._send_registration(reader, writer)

    async def _send_registration(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """Send the file list, in pages if it is large. The caller holds the lock."""
        entries = []
        for shared in self.index.values():
            meta = shared.to_meta()
            meta["name"] = meta["name"][:MAX_NAME_LENGTH]
            entries.append(meta)
        for number, page in enumerate(_paginate(entries, REGISTER_PAGE_BYTES)):
            reply = await self._exchange(reader, writer, {"type": "register", "files": page, "append": number > 0})
            if reply["type"] != "ok":
                raise TrackerError(f"unexpected reply to register: {reply['type']}")

    async def _request(self, message: Message) -> Message:
        """Send one message on the live tracker link and return its reply."""
        async with self._lock:
            # Checked *inside* the lock: whoever held it before us may have
            # lost the connection, and the link may even have been replaced.
            reader, writer = self._live_link()
            return await self._exchange(reader, writer, message)

    def _live_link(self) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        if not self.connected or self._reader is None or self._writer is None:
            raise TrackerError("not connected to the tracker (reconnecting in the background)")
        return self._reader, self._writer

    async def _exchange(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, message: Message
    ) -> Message:
        """One request and its reply on a given link. The caller holds the lock."""
        try:
            frame = encode(message)
        except FrameTooLarge as exc:
            raise TrackerError(f"request too large to send ({exc})") from exc
        try:
            writer.write(frame)
            await asyncio.wait_for(writer.drain(), self.request_timeout)
            reply = await read_message(reader, timeout=self.request_timeout)
        except (ConnectionClosed, TimeoutError, OSError) as exc:
            await self._drop(writer)
            raise TrackerError(f"lost the tracker connection ({describe(exc)})") from exc
        except ProtocolError as exc:
            await self._drop(writer)
            raise TrackerError(f"tracker sent something unreadable: {exc}") from exc
        if reply["type"] == "error":
            raise TrackerError(f"{reply.get('code', 'error')}: {reply.get('message', '')}")
        return reply

    async def _drop(self, writer: asyncio.StreamWriter) -> None:
        """Drop a link. If it was the live one, note that we are offline."""
        if writer is self._writer:
            self.connected = False
            self._reader = self._writer = None
            self._link_lost.set()
        # Abort rather than close: a link is dropped because it failed, and a
        # polite close would wait for a tracker that may no longer be reading.
        await close_writer(writer, abort=True)

    async def _disconnect_tracker(self) -> None:
        if self._writer is not None:
            await self._drop(self._writer)

    async def _keepalive(self) -> None:
        """Ping the tracker on a timer; reconnect (and re-register) if the link dies.

        This task must never end on its own: without it a peer that lost its
        tracker would stay offline for good.
        """
        backoff = self.reconnect_delay
        while True:
            try:
                if not self.connected:
                    try:
                        await self._connect_tracker()
                    except TrackerError as exc:
                        log.warning("tracker reconnect failed: %s; retrying in %.1fs", exc, backoff)
                        await asyncio.sleep(backoff)
                        backoff = min(backoff * 2, 30.0)
                        continue
                    backoff = self.reconnect_delay
                    log.info("reconnected to tracker as %s", self.peer_id)
                    continue
                try:
                    # Sleep until the next heartbeat, or wake at once if another
                    # request on the link noticed it die.
                    await asyncio.wait_for(self._link_lost.wait(), self.heartbeat_interval)
                    continue
                except TimeoutError:
                    pass
                try:
                    reply = await self._request({"type": "ping"})
                    if reply["type"] != "pong":
                        log.warning("tracker answered ping with %r", reply["type"])
                except TrackerError as exc:
                    log.warning("lost tracker connection: %s", exc)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("keepalive hit an unexpected error; carrying on")
                await asyncio.sleep(backoff)

    # ---------------------------------------------------------- user actions --

    async def search(self, query: str) -> list[Result]:
        reply = await self._request({"type": "search", "query": query})
        items = reply.get("results")
        if not isinstance(items, list):
            raise TrackerError("tracker sent a malformed search reply")
        return _parse_results(items)

    async def lookup(self, sha256: str) -> Result | None:
        """Every peer currently offering this exact file, or None if nobody is."""
        reply = await self._request({"type": "lookup", "sha256": sha256})
        if not reply.get("sources"):
            return None
        return _parse_results([reply])[0]

    async def download(self, result: Result, progress: ProgressCallback | None = None) -> Path:
        """Try each source in turn until one delivers the file intact."""
        lock = self._download_locks.setdefault(result.sha256, asyncio.Lock())
        async with lock:  # two downloads of one file would both append to the same partial
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
                    log.debug("%s", exc)  # the caller gets the full list of failures
                    failures.append(str(exc))
        if not failures:
            raise TransferError("no other peer is sharing that file")
        raise TransferError("every source failed:\n  " + "\n  ".join(failures))

    async def rescan(self) -> int:
        """Re-hash the share folder and tell the tracker about the new list."""
        self.index = await scan_directory_async(self.share_dir)
        if self.connected:
            await self._register()
        return len(self.index)

    async def _on_incoming(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self._incoming.add(writer)
        try:
            if self._upload_slots.locked():  # every slot is taken
                abort = False
                try:
                    await asyncio.wait_for(
                        write_message(
                            writer,
                            protocol.error(
                                "busy", f"this peer is already sending {self.max_uploads} files; try again soon"
                            ),
                        ),
                        5.0,
                    )
                except (OSError, TimeoutError):
                    abort = True
                finally:
                    await close_writer(writer, abort=abort)
                return
            async with self._upload_slots:
                await serve_file(self.index, reader, writer)
        finally:
            self._incoming.discard(writer)


def _paginate(entries: list[Message], budget: int) -> list[list[Message]]:
    """Split a file list into pages whose encoded size stays under ``budget``.

    Always returns at least one page, possibly empty, because an empty
    ``register`` is how a peer says it now shares nothing.
    """
    pages: list[list[Message]] = [[]]
    used = 0
    for entry in entries:
        cost = len(json.dumps(entry, separators=(",", ":")).encode("utf-8")) + 1
        if pages[-1] and used + cost > budget:
            pages.append([])
            used = 0
        pages[-1].append(entry)
        used += cost
    return pages


def _parse_results(items: list) -> list[Result]:
    try:
        return [Result.from_dict(item) for item in items]
    except (AttributeError, KeyError, TypeError, ValueError) as exc:
        raise TrackerError(f"tracker sent a malformed result ({describe(exc)})") from exc
