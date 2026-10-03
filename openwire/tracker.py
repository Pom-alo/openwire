"""The index server: Napster's half of the design.

Napster was not peer-to-peer all the way down. Discovery was centralised:
every client kept one TCP connection open to a Napster server, told it which
files it was sharing, and sent its searches there. Only the file transfer
itself went directly between two users. openwire's tracker plays that server
role.

Consequences of this design, all deliberate and all worth noticing:

* Search is instant and exact, because one process sees the whole index.
* The index is only as alive as the connections. When a peer's connection
  closes, its files vanish from results immediately. Nothing is persisted.
* The server is a single point of failure and a single point of control,
  which is exactly why the Gnutella generation (Phase 3) removed it.

The tracker never touches file contents. It stores names, sizes, hashes and
addresses, and it answers questions about them.
"""

from __future__ import annotations

import asyncio
import logging
import secrets
import time
from dataclasses import dataclass, field

from . import protocol
from .protocol import (
    PROTOCOL_VERSION,
    ConnectionClosed,
    Message,
    ProtocolError,
    close_writer,
    normalize_sha256,
    read_message,
    write_message,
)

log = logging.getLogger("openwire.tracker")

DEFAULT_PORT = 8888  # Napster's servers listened on 8888 (and 7777, 4444, ...)

MAX_FILES_PER_PEER = 50_000
MAX_NAME_LENGTH = 512
MAX_NICK_LENGTH = 32
MAX_QUERY_LENGTH = 200
MAX_RESULTS = 50
MAX_FILE_SIZE = 1 << 53  # the largest integer JSON carries exactly


class BadRequest(Exception):
    """A well-formed message with unacceptable contents."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


@dataclass
class FileMeta:
    name: str
    size: int


@dataclass
class PeerRecord:
    peer_id: str
    nick: str
    host: str
    port: int
    files: dict[str, FileMeta] = field(default_factory=dict)
    connected_at: float = field(default_factory=time.monotonic)

    def as_source(self) -> Message:
        return {"peer_id": self.peer_id, "nick": self.nick, "host": self.host, "port": self.port}


class Tracker:
    def __init__(self, host: str = "127.0.0.1", port: int = DEFAULT_PORT, *, idle_timeout: float = 90.0) -> None:
        self.host = host
        self.port = port
        self.idle_timeout = idle_timeout
        self.peers: dict[str, PeerRecord] = {}
        self._server: asyncio.base_events.Server | None = None
        self._connections: set[asyncio.StreamWriter] = set()

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._handle_connection, self.host, self.port)
        self.port = self._server.sockets[0].getsockname()[1]
        log.info("tracker listening on %s:%d", self.host, self.port)

    async def serve_forever(self) -> None:
        assert self._server is not None, "call start() first"
        await self._server.serve_forever()

    async def close(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
        # Drop the live peers too. Closing our end makes each handler's pending
        # read fail, which runs its cleanup and withdraws the peer's files.
        for writer in list(self._connections):
            writer.close()
        self._connections.clear()

    @property
    def file_count(self) -> int:
        return sum(len(peer.files) for peer in self.peers.values())

    # ------------------------------------------------------- connections --

    async def _handle_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """One coroutine per connected peer, alive as long as the connection is."""
        peername = writer.get_extra_info("peername")
        observed_host = peername[0] if peername else "unknown"
        record: PeerRecord | None = None
        self._connections.add(writer)
        try:
            while True:
                message = await read_message(reader, timeout=self.idle_timeout)
                kind = message["type"]
                if kind == "bye":
                    break
                if record is None:
                    # The first message must introduce the peer.
                    if kind != "hello":
                        await write_message(
                            writer, protocol.error("hello_required", "say hello before anything else")
                        )
                        break
                    record = self._hello(message, observed_host)
                    await write_message(
                        writer,
                        {
                            "type": "welcome",
                            "version": PROTOCOL_VERSION,
                            "peer_id": record.peer_id,
                            "your_host": record.host,
                        },
                    )
                    continue
                try:
                    reply = self._dispatch(record, message)
                except BadRequest as exc:
                    reply = protocol.error("bad_request", exc.message)
                await write_message(writer, reply)
        except BadRequest as exc:  # a malformed hello
            try:
                await write_message(writer, protocol.error("bad_request", exc.message))
            except OSError:
                pass
        except ConnectionClosed:
            pass
        except TimeoutError:
            log.info("%s idle for %.0fs, dropping", _who(record, observed_host), self.idle_timeout)
        except ProtocolError as exc:
            log.warning("%s broke protocol: %s", _who(record, observed_host), exc)
        except OSError as exc:
            log.debug("%s connection error: %s", _who(record, observed_host), protocol.describe(exc))
        finally:
            self._connections.discard(writer)
            if record is not None:
                # This is the whole liveness model: no connection, no files.
                self.peers.pop(record.peer_id, None)
                log.info(
                    "peer %s (%s) left; %d files withdrawn", record.peer_id, record.nick, len(record.files)
                )
            await close_writer(writer)

    # ---------------------------------------------------------- messages --

    def _hello(self, message: Message, observed_host: str) -> PeerRecord:
        port = _require_int(message, "port", 1, 65535)
        nick = message.get("nick") or ""
        if not isinstance(nick, str) or len(nick) > MAX_NICK_LENGTH or not nick.isprintable():
            raise BadRequest(f"nick must be printable text of at most {MAX_NICK_LENGTH} characters")
        # Like Napster, we normally record the address the connection came from:
        # the peer may not know its own reachable address, but we can see it.
        host = message.get("host", observed_host)
        if not isinstance(host, str) or not host or len(host) > 253:
            raise BadRequest("host must be a non-empty string")
        peer_id = secrets.token_hex(6)
        record = PeerRecord(peer_id=peer_id, nick=nick or peer_id, host=host, port=port)
        self.peers[peer_id] = record
        log.info("peer %s (%s) joined, reachable at %s:%d", peer_id, record.nick, host, port)
        return record

    def _dispatch(self, record: PeerRecord, message: Message) -> Message:
        kind = message["type"]
        if kind == "register":
            return self._register(record, message)
        if kind == "search":
            return self._search(record, message)
        if kind == "lookup":
            return self._lookup(record, message)
        if kind == "ping":
            return {"type": "pong"}
        if kind == "hello":
            raise BadRequest("already said hello")
        return protocol.error("unknown_type", f"unknown message type {kind!r}")

    def _register(self, record: PeerRecord, message: Message) -> Message:
        """Replace the peer's whole file list with the one in the message."""
        files = message.get("files")
        if not isinstance(files, list):
            raise BadRequest("files must be a list")
        if len(files) > MAX_FILES_PER_PEER:
            raise BadRequest(f"at most {MAX_FILES_PER_PEER} files per peer")
        parsed: dict[str, FileMeta] = {}
        for entry in files:
            if not isinstance(entry, dict):
                raise BadRequest("each file must be an object")
            digest = normalize_sha256(entry.get("sha256"))
            if digest is None:
                raise BadRequest("each file needs a 64-hex-character sha256")
            name = entry.get("name")
            if not isinstance(name, str) or not name.strip() or len(name) > MAX_NAME_LENGTH:
                raise BadRequest(f"each file needs a name of 1 to {MAX_NAME_LENGTH} characters")
            size = _require_int(entry, "size", 0, MAX_FILE_SIZE)
            parsed[digest] = FileMeta(name=name, size=size)
        record.files = parsed
        log.info("peer %s (%s) registered %d files", record.peer_id, record.nick, len(parsed))
        return {"type": "ok", "count": len(parsed)}

    def _search(self, record: PeerRecord, message: Message) -> Message:
        """Case-insensitive match: every word in the query must appear in the name."""
        query = message.get("query")
        if not isinstance(query, str):
            raise BadRequest("query must be a string")
        if len(query) > MAX_QUERY_LENGTH:
            raise BadRequest(f"query longer than {MAX_QUERY_LENGTH} characters")
        terms = query.lower().split()
        if not terms:
            raise BadRequest("query is empty")

        # Group by content hash so one result lists every peer that has the
        # file. Napster grouped nothing, which is why results were a wall of
        # near-duplicates.
        hits: dict[str, Message] = {}
        for peer in self.peers.values():
            if peer.peer_id == record.peer_id:
                continue  # a peer never needs to download from itself
            for digest, meta in peer.files.items():
                haystack = meta.name.lower()
                if all(term in haystack for term in terms):
                    hit = hits.get(digest)
                    if hit is None:
                        hit = hits[digest] = {"sha256": digest, "name": meta.name, "size": meta.size, "sources": []}
                    hit["sources"].append(peer.as_source())
        ordered = sorted(hits.values(), key=lambda h: (-len(h["sources"]), h["name"].lower()))
        return {"type": "results", "query": query, "total": len(ordered), "results": ordered[:MAX_RESULTS]}

    def _lookup(self, record: PeerRecord, message: Message) -> Message:
        """Who has this exact file right now? Used for retries and for downloading by hash."""
        digest = normalize_sha256(message.get("sha256"))
        if digest is None:
            raise BadRequest("sha256 must be 64 hex characters")
        name: str | None = None
        size: int | None = None
        sources: list[Message] = []
        for peer in self.peers.values():
            if peer.peer_id == record.peer_id:
                continue
            meta = peer.files.get(digest)
            if meta is None:
                continue
            if name is None:
                name, size = meta.name, meta.size
            sources.append(peer.as_source())
        return {"type": "sources", "sha256": digest, "name": name, "size": size, "sources": sources}


def _require_int(obj: Message, key: str, low: int, high: int) -> int:
    value = obj.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or not low <= value <= high:
        raise BadRequest(f"{key} must be an integer from {low} to {high}")
    return value


def _who(record: PeerRecord | None, observed_host: str) -> str:
    return f"peer {record.peer_id} ({record.nick})" if record else f"connection from {observed_host}"
