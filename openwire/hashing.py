"""Content hashing and share-folder scanning.

A file in openwire is identified by the SHA-256 of its bytes, never by its
name. Two peers sharing the same song under different file names are sharing
*one* file, and a downloader can prove it received exactly that file by
hashing what arrived. Napster matched on file names, which is why the same
track showed up a hundred times in results and why mislabelled files were a
constant problem. Content addressing fixes both, and it is the foundation
that multi-source downloads (Phase 2) are built on.
"""

from __future__ import annotations

import asyncio
import hashlib
import os
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

CHUNK_SIZE = 1024 * 1024


def hash_file(path: Path) -> tuple[str, int]:
    """Return (hex SHA-256, byte count) of a file, reading it in chunks."""
    hasher = hashlib.sha256()
    size = 0
    with path.open("rb") as handle:
        while chunk := handle.read(CHUNK_SIZE):
            hasher.update(chunk)
            size += len(chunk)
    return hasher.hexdigest(), size


def sha256_file(path: Path) -> str:
    return hash_file(path)[0]


def hash_prefix(path: Path, nbytes: int) -> "hashlib._Hash":
    """Return a hasher that has consumed the first ``nbytes`` of ``path``.

    Used when resuming a partial download: we hash what is already on disk so
    the final digest covers the whole file, not just the bytes fetched in this
    session.
    """
    hasher = hashlib.sha256()
    remaining = max(nbytes, 0)
    if remaining == 0:
        return hasher
    with path.open("rb") as handle:
        while remaining > 0:
            chunk = handle.read(min(CHUNK_SIZE, remaining))
            if not chunk:
                break
            hasher.update(chunk)
            remaining -= len(chunk)
    return hasher


@dataclass(frozen=True)
class SharedFile:
    path: Path   # where it lives on this machine; never sent over the wire
    name: str    # path relative to the share folder, with forward slashes
    size: int
    sha256: str

    def to_meta(self) -> dict:
        return {"name": self.name, "size": self.size, "sha256": self.sha256}


def iter_shareable(root: Path) -> Iterator[Path]:
    """Yield the regular files under ``root`` that a peer shares.

    Hidden files and folders (dot-prefixed) and symlinks are skipped, in a
    stable order: a folder's own files first, then its subfolders.
    """
    root = Path(root).resolve()
    if not root.is_dir():
        return
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = sorted(d for d in dirnames if not d.startswith("."))
        for filename in sorted(filenames):
            if filename.startswith("."):
                continue
            path = Path(dirpath) / filename
            if path.is_symlink() or not path.is_file():
                continue
            yield path


def _index_one(index: dict[str, SharedFile], root: Path, path: Path) -> None:
    try:
        digest, size = hash_file(path)
    except OSError:
        return
    if digest in index:
        return  # identical content already indexed: one copy is all a peer needs to serve
    index[digest] = SharedFile(path=path, name=path.relative_to(root).as_posix(), size=size, sha256=digest)


def scan_directory(root: Path) -> dict[str, SharedFile]:
    """Hash every shareable file under ``root``; return them keyed by SHA-256."""
    root = Path(root).resolve()
    index: dict[str, SharedFile] = {}
    for path in iter_shareable(root):
        _index_one(index, root, path)
    return index


async def scan_directory_async(root: Path) -> dict[str, SharedFile]:
    """Like :func:`scan_directory`, but hashes one file per worker-thread hop.

    Hashing is CPU and disk bound and must stay off the event loop. Doing it
    file by file, rather than in one long thread call, keeps the loop (and
    Ctrl-C) responsive between files.
    """
    root = Path(root).resolve()
    index: dict[str, SharedFile] = {}
    for path in await asyncio.to_thread(lambda: list(iter_shareable(root))):
        await asyncio.to_thread(_index_one, index, root, path)
    return index
