"""Moving the actual bytes between two peers.

This is the one genuinely peer-to-peer part of Phase 1. The tracker only ever
hands out addresses; the file itself travels straight from the peer that has
it to the peer that wants it, over a fresh TCP connection that carries exactly
one transfer and then closes (the one-request-per-connection model of
HTTP/1.0, and of Napster's own data connections)::

    downloader -> seeder   {"type": "get", "sha256": "...", "offset": 0}
    seeder -> downloader   {"type": "file", "sha256": "...", "name": "...", "size": N}
    seeder -> downloader   N - offset raw bytes, then closes the connection
or
    seeder -> downloader   {"type": "error", "code": "not_found", "message": "..."}

The ``offset`` field gives us resumable downloads almost for free: a partial
file is kept at ``downloads/.partial/<sha256>.part`` and the next attempt
asks for the rest.

Things a downloader must never trust from the network:

* the file **name**, which is reduced to a bare, filesystem-safe filename
  before it touches the disk (a hostile peer could otherwise name a file
  ``../../.bashrc``);
* the **size**, which must match what the tracker reported;
* the **bytes**, which are verified against the SHA-256 we asked for before
  the file is given its final name.

The seeder, for its part, only ever serves files by hash from its own index.
There is no way to ask it for a path, so there is no path to traverse.
"""

from __future__ import annotations

import asyncio
import hashlib
import itertools
import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path

from . import protocol
from .hashing import SharedFile, hash_prefix
from .protocol import (
    ConnectionClosed,
    ProtocolError,
    close_writer,
    describe,
    normalize_sha256,
    read_message,
    write_message,
)

log = logging.getLogger("openwire.transfer")

CHUNK_SIZE = 64 * 1024
PARTIAL_DIRNAME = ".partial"

#: Called with (bytes_received_so_far, total_bytes) as a download progresses.
ProgressCallback = Callable[[int, int], None]


class TransferError(Exception):
    """A download attempt from one particular source failed."""


@dataclass(frozen=True)
class Source:
    """Where a file can be fetched from: one peer's advertised address."""

    peer_id: str
    nick: str
    host: str
    port: int

    @classmethod
    def from_dict(cls, data: Mapping) -> "Source":
        """Build from a tracker's description of a peer, refusing anything malformed."""
        if not isinstance(data, Mapping):
            raise TypeError("a source must be an object")
        peer_id, host, port = data.get("peer_id"), data.get("host"), data.get("port")
        if not isinstance(peer_id, str) or not peer_id:
            raise ValueError("a source needs a peer_id")
        if not isinstance(host, str) or not host:
            raise ValueError("a source needs a host")
        if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
            raise ValueError("a source needs a port from 1 to 65535")
        nick = data.get("nick")
        return cls(peer_id=peer_id, nick=nick if isinstance(nick, str) and nick else peer_id, host=host, port=port)

    def __str__(self) -> str:
        return f"{self.nick}@{self.host}:{self.port}"


# ------------------------------------------------------------- seeding --


async def serve_file(
    index: Mapping[str, SharedFile],
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    *,
    request_timeout: float = 15.0,
    write_timeout: float = 30.0,
) -> None:
    """Handle one incoming transfer connection: read a ``get``, stream the file.

    ``write_timeout`` bounds how long we wait for the downloader to take each
    chunk. Without it, a client that connects and then stops reading would pin
    this handler, and the open file, forever.
    """
    peername = writer.get_extra_info("peername")
    failed = False
    try:
        request = await read_message(reader, timeout=request_timeout)
        if request["type"] != "get":
            await write_message(writer, protocol.error("bad_request", "expected a get message"))
            return
        digest = normalize_sha256(request.get("sha256"))
        offset = request.get("offset", 0)
        if digest is None or isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
            await write_message(
                writer, protocol.error("bad_request", "need a valid sha256 and a non-negative integer offset")
            )
            return
        shared = index.get(digest)
        if shared is None:
            await write_message(writer, protocol.error("not_found", "this peer is not sharing that file"))
            return
        if offset > shared.size:
            await write_message(
                writer,
                protocol.error("bad_offset", f"offset {offset} is past the end of a {shared.size}-byte file"),
            )
            return
        try:
            handle = shared.path.open("rb")
        except OSError:
            await write_message(writer, protocol.error("unavailable", "the file is no longer readable here"))
            return

        with handle:
            header = {"type": "file", "sha256": shared.sha256, "name": shared.name, "size": shared.size}
            await asyncio.wait_for(write_message(writer, header), write_timeout)
            log.info(
                "sending %s (%d bytes from offset %d) to %s", shared.name, shared.size - offset, offset, peername
            )
            handle.seek(offset)
            remaining = shared.size - offset
            while remaining > 0:
                chunk = handle.read(min(CHUNK_SIZE, remaining))
                if not chunk:
                    # The file shrank since it was indexed. Stop here; the
                    # downloader's hash check will reject what it got.
                    break
                writer.write(chunk)
                await asyncio.wait_for(writer.drain(), write_timeout)
                remaining -= len(chunk)
    except (ConnectionClosed, TimeoutError, ProtocolError, OSError) as exc:
        failed = True
        log.debug("transfer to %s ended early: %s", peername, describe(exc))
    finally:
        # After a failure there may be unsent data queued for a client that is
        # not reading; a polite close would wait on it forever.
        await close_writer(writer, abort=failed)


# ---------------------------------------------------------- downloading --


async def download(
    source: Source,
    *,
    sha256: str,
    size: int,
    name: str,
    dest_dir: Path,
    progress: ProgressCallback | None = None,
    connect_timeout: float = 10.0,
    read_timeout: float = 30.0,
) -> Path:
    """Fetch one file from one source; return the path it was saved to.

    Raises :class:`TransferError` if anything goes wrong. A partial file is
    kept for a later resume unless its contents proved to be wrong.
    """
    digest = normalize_sha256(sha256)
    if digest is None:
        raise ValueError("sha256 must be 64 hex characters")
    if isinstance(size, bool) or not isinstance(size, int) or size < 0:
        raise ValueError("size must be a non-negative integer")

    dest_dir = Path(dest_dir)
    partial_dir = dest_dir / PARTIAL_DIRNAME
    part = partial_dir / f"{digest}.part"
    try:
        partial_dir.mkdir(parents=True, exist_ok=True)
        # Resume: whatever is already on disk counts, as long as it is not too long.
        offset = part.stat().st_size if part.exists() else 0
        if offset > size:
            part.unlink()
            offset = 0
        hasher = hash_prefix(part, offset) if offset else hashlib.sha256()
    except OSError as exc:
        raise TransferError(f"cannot use download folder {dest_dir}: {describe(exc)}") from exc

    try:
        reader, writer = await asyncio.wait_for(asyncio.open_connection(source.host, source.port), connect_timeout)
    except (OSError, TimeoutError) as exc:
        raise TransferError(f"could not connect to {source}: {describe(exc)}") from exc

    try:
        await write_message(writer, {"type": "get", "sha256": digest, "offset": offset})
        header = await read_message(reader, timeout=read_timeout)
        if header["type"] == "error":
            raise TransferError(f"{source} refused: {header.get('code')}: {header.get('message')}")
        if header["type"] != "file" or header.get("sha256") != digest:
            raise TransferError(f"{source} sent an unexpected reply ({header['type']})")
        if header.get("size") != size:
            raise TransferError(f"{source} reports size {header.get('size')}, the tracker said {size}")

        remaining = size - offset
        if progress is not None:
            progress(offset, size)
        with part.open("ab") as handle:
            while remaining > 0:
                chunk = await asyncio.wait_for(reader.read(min(CHUNK_SIZE, remaining)), read_timeout)
                if not chunk:
                    raise TransferError(f"{source} closed the connection with {remaining} bytes still to come")
                handle.write(chunk)
                hasher.update(chunk)
                remaining -= len(chunk)
                if progress is not None:
                    progress(size - remaining, size)
    except (ConnectionClosed, TimeoutError, ProtocolError, OSError) as exc:
        raise TransferError(f"transfer from {source} failed: {describe(exc)}") from exc
    finally:
        await close_writer(writer)

    try:
        on_disk = part.stat().st_size
        if on_disk != size:
            # The hash above covers what came over the wire, not what is on
            # disk. If they differ, something else wrote to the partial file
            # (a second download of the same hash, for instance).
            part.unlink(missing_ok=True)
            raise TransferError(
                f"partial file is {on_disk} bytes but the file is {size}; another download may be writing it"
            )
        if hasher.hexdigest() != digest:
            # Wrong bytes. Throw them away so the next attempt starts clean.
            part.unlink(missing_ok=True)
            raise TransferError(f"hash mismatch: the bytes from {source} are not the file we asked for")
        final = unique_path(dest_dir / sanitize_filename(name, fallback=digest[:12]))
        part.replace(final)
    except OSError as exc:
        raise TransferError(f"downloaded, but cannot save into {dest_dir}: {describe(exc)}") from exc
    return final


# ------------------------------------------------------------ filenames --

_ILLEGAL_CHARS = set('<>:"/\\|?*') | {chr(code) for code in range(32)}
_RESERVED_STEMS = {
    "CON", "PRN", "AUX", "NUL",
    *(f"COM{i}" for i in range(1, 10)),
    *(f"LPT{i}" for i in range(1, 10)),
}
MAX_FILENAME_LENGTH = 200


def sanitize_filename(name: str, *, fallback: str) -> str:
    """Reduce an untrusted name to a bare filename that is safe on every OS.

    Directory components are dropped; characters Windows forbids and anything
    non-printing (control codes, bidi overrides that make ``exe`` read as
    ``txt``) become ``_``; leading and trailing dots and spaces go (so a
    download is never a hidden file); and Windows' reserved device names
    (``CON``, ``NUL``...) get a leading underscore. If nothing usable remains,
    the ``fallback`` (typically a hash prefix) is used instead.
    """
    base = name.replace("\\", "/").rsplit("/", 1)[-1]
    cleaned = "".join("_" if ch in _ILLEGAL_CHARS or not ch.isprintable() else ch for ch in base).strip(" .")
    if not cleaned:
        cleaned = fallback
    if cleaned.split(".", 1)[0].upper() in _RESERVED_STEMS:
        cleaned = "_" + cleaned
    return cleaned[:MAX_FILENAME_LENGTH]


def unique_path(path: Path) -> Path:
    """Return ``path`` or, if it exists, ``name (1).ext``, ``name (2).ext``..."""
    if not path.exists():
        return path
    for counter in itertools.count(1):
        candidate = path.with_name(f"{path.stem} ({counter}){path.suffix}")
        if not candidate.exists():
            return candidate
    raise AssertionError("unreachable")
