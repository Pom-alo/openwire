"""The interactive shell, driven by a scripted stand-in for stdin."""

import io

from openwire.shell import Shell, fmt_size
from tests.support import make_file, run, start_peer, start_tracker


class Script:
    def __init__(self, *lines):
        self.lines = list(lines)
        self.prompts = 0

    async def readline(self, prompt):
        self.prompts += 1
        return (self.lines.pop(0) + "\n") if self.lines else None


def test_fmt_size():
    assert fmt_size(0) == "0 B"
    assert fmt_size(1023) == "1023 B"
    assert fmt_size(1024) == "1.0 KiB"
    assert fmt_size(3 * 1024 * 1024 + 300_000) == "3.3 MiB"


def test_shell_search_get_and_friends(tmp_path):
    async def go():
        tracker = await start_tracker()
        peers = []
        try:
            _, data = make_file(tmp_path / "alice/share", "summer 99.mp3", 40_000)
            alice = await start_peer(tracker, tmp_path / "alice/share", tmp_path / "alice/dl", "alice")
            bob = await start_peer(tracker, tmp_path / "bob/share", tmp_path / "bob/dl", "bob")
            peers += [alice, bob]

            out = io.StringIO()
            script = Script(
                "", "help", "ls", "search", "search nothing-here", "search summer", "get 9", "get banana", "get 1",
                "get " + "0" * 64, "status", "rescan", "dance", "quit", "never reached",
            )
            await Shell(bob, out=out).run(script)
            return out.getvalue(), script, data
        finally:
            for peer in peers:
                await peer.close()
            await tracker.close()

    text, script, data = run(go())
    assert "Commands:" in text
    assert "sharing nothing" in text
    assert "usage: search" in text
    assert "no matches" in text
    assert "summer 99.mp3" in text and "get <#> to download" in text
    assert "pick 1 to 1" in text
    assert "usage: get" in text
    assert "saved to" in text
    assert (tmp_path / "bob/dl/summer 99.mp3").read_bytes() == data
    assert "nobody is sharing that file" in text
    assert "connected" in text
    assert "sharing 0 files" in text
    assert "unknown command 'dance'" in text
    assert script.lines == ["never reached"]


def test_shell_exits_on_eof(tmp_path):
    async def go():
        tracker = await start_tracker()
        bob = await start_peer(tracker, tmp_path / "bob/share", tmp_path / "bob/dl", "bob")
        try:
            out = io.StringIO()
            await Shell(bob, out=out).run(Script())
            return out.getvalue()
        finally:
            await bob.close()
            await tracker.close()

    text = run(go())
    assert "openwire" in text and "Type 'help'" in text
