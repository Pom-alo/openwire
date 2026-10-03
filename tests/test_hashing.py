import hashlib

from openwire.hashing import hash_file, hash_prefix, scan_directory, sha256_file
from tests.support import make_file


def test_hash_file_matches_hashlib(tmp_path):
    path, data = make_file(tmp_path, "blob.bin", 3 * 1024 * 1024 + 17)
    digest, size = hash_file(path)
    assert digest == hashlib.sha256(data).hexdigest()
    assert size == len(data)
    assert sha256_file(path) == digest


def test_hash_prefix_can_be_continued(tmp_path):
    path, data = make_file(tmp_path, "blob.bin", 100_000)
    assert hash_prefix(path, 0).hexdigest() == hashlib.sha256(b"").hexdigest()
    assert hash_prefix(path, 12_345).hexdigest() == hashlib.sha256(data[:12_345]).hexdigest()
    hasher = hash_prefix(path, 50_000)
    hasher.update(data[50_000:])
    assert hasher.hexdigest() == hashlib.sha256(data).hexdigest()


def test_scan_directory_indexes_by_content(tmp_path):
    share = tmp_path / "share"
    _, song = make_file(share, "Albums/Live/track01.mp3", 2048)
    (share / "copy-of-track01.mp3").write_bytes(song)  # same content, different name
    make_file(share, "notes.txt", 10)
    make_file(share, ".hidden.txt", 10)
    make_file(share, ".git/objects/abc", 10)
    (share / "empty.bin").write_bytes(b"")

    index = scan_directory(share)

    # Top-level files are walked before subfolders, so the top-level copy is the
    # one kept and the identical file under Albums/ is treated as a duplicate.
    assert sorted(f.name for f in index.values()) == ["copy-of-track01.mp3", "empty.bin", "notes.txt"]
    song_digest = hashlib.sha256(song).hexdigest()
    assert index[song_digest].size == 2048
    assert index[song_digest].path == (share / "copy-of-track01.mp3").resolve()
    for digest, shared in index.items():
        assert shared.sha256 == digest
        assert shared.to_meta() == {"name": shared.name, "size": shared.size, "sha256": digest}


def test_scan_directory_uses_forward_slashes_for_nested_names(tmp_path):
    share = tmp_path / "share"
    make_file(share, "a/b/c.txt", 5)
    (shared,) = scan_directory(share).values()
    assert shared.name == "a/b/c.txt"


def test_scan_directory_missing_or_empty(tmp_path):
    assert scan_directory(tmp_path / "nope") == {}
    (tmp_path / "empty").mkdir()
    assert scan_directory(tmp_path / "empty") == {}
