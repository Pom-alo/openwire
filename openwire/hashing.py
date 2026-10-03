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

import hashlib
import os
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


def scan_directory(root: Path) -> dict[str, SharedFile]:
    """Hash every regular file under ``root``; return them keyed by SHA-256.

    Hidden files and folders (dot-prefixed) and symlinks are skipped. If two
    files have identical content only the first is kept: the index is a set
    of *contents*, and one copy is all a peer needs to serve.
    """
    index: dict[str, SharedFile] = {}
    root = Path(root).resolve()
    if not root.is_dir():
        return index
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = sorted(d for d in dirnames if not d.startswith("."))
        for filename in sorted(filenames):
            if filename.startswith("."):
                continue
            path = Path(dirpath) / filename
            if path.is_symlink() or not path.is_file():
                continue
            try:
                digest, size = hash_file(path)
            except OSError:
                continue
            if digest in index:
                continue
            index[digest] = SharedFile(
                path=path, name=path.relative_to(root).as_posix(), size=size, sha256=digest
            )
    return index
