"""The interactive command line a running peer presents to its user."""

from __future__ import annotations

import asyncio
import logging
import sys
import threading
import time
from typing import TextIO

from . import __version__
from .peer import Peer, Result, TrackerError
from .protocol import describe, normalize_sha256
from .transfer import TransferError

log = logging.getLogger("openwire.shell")

HELP = """\
Commands:
  search <words>    ask the tracker who has files whose names contain all the words
  get <#|sha256>    download result # from the last search, or any file by its hash
  ls                list the files you are sharing
  status            show your peer id, address, tracker and counts
  rescan            re-hash the share folder and re-register with the tracker
  help              this text
  quit              leave (your files disappear from the index immediately)
"""


def fmt_size(n: int) -> str:
    value = float(n)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if value < 1024 or unit == "TiB":
            return f"{int(value)} {unit}" if unit == "B" else f"{value:.1f} {unit}"
        value /= 1024
    raise AssertionError("unreachable")


def safe_text(text: str) -> str:
    """Make a string that came from the network safe to print.

    Control characters and other non-printing code points (terminal escape
    sequences, bidi overrides, line breaks) become ``?`` so a file name
    cannot repaint or fake the terminal.
    """
    return "".join(ch if ch.isprintable() else "?" for ch in text)


class StdinLines:
    """Feed lines from stdin into the asyncio loop without blocking it.

    ``input()`` would block the whole event loop, and running it in the default
    thread pool would hang interpreter shutdown until the user pressed Enter.
    A daemon thread that pushes lines onto a queue sidesteps both problems.
    """

    def __init__(self, stream: TextIO | None = None) -> None:
        self._stream = stream or sys.stdin
        self._loop = asyncio.get_running_loop()
        self._queue: asyncio.Queue[str | None] = asyncio.Queue()
        threading.Thread(target=self._pump, name="openwire-stdin", daemon=True).start()

    def _pump(self) -> None:
        try:
            for line in self._stream:
                self._loop.call_soon_threadsafe(self._queue.put_nowait, line)
        except (ValueError, OSError):
            pass
        finally:
            try:
                self._loop.call_soon_threadsafe(self._queue.put_nowait, None)
            except RuntimeError:
                pass  # loop already closed

    async def readline(self, prompt: str) -> str | None:
        print(prompt, end="", flush=True)
        return await self._queue.get()


class Shell:
    def __init__(self, peer: Peer, *, out: TextIO | None = None) -> None:
        self.peer = peer
        self.out = out or sys.stdout
        self.results: list[Result] = []

    def say(self, text: str = "") -> None:
        print(text, file=self.out, flush=True)

    async def run(self, lines: StdinLines | None = None) -> None:
        lines = lines or StdinLines()
        self.banner()
        while True:
            line = await lines.readline("openwire> ")
            if line is None:
                self.say()
                return
            parts = line.split(None, 1)  # any whitespace separates command and argument
            if not parts:
                continue
            command = parts[0]
            arg = parts[1].strip() if len(parts) > 1 else ""
            try:
                if not await self.dispatch(command.lower(), arg):
                    return
            except TrackerError as exc:
                self.say(f"tracker: {exc}")
            except TransferError as exc:
                self.say(f"download failed: {exc}")
            except Exception as exc:  # a bug or a surprise from the OS must not end the session
                log.debug("command %r failed", command, exc_info=True)
                self.say(f"error: {describe(exc)}")

    async def dispatch(self, command: str, arg: str) -> bool:
        """Run one command; return False when the user wants to leave."""
        if command in ("quit", "exit", "q"):
            return False
        if command == "help":
            self.say(HELP)
        elif command == "search":
            await self.cmd_search(arg)
        elif command == "get":
            await self.cmd_get(arg)
        elif command == "ls":
            self.cmd_ls()
        elif command == "status":
            self.cmd_status()
        elif command == "rescan":
            count = await self.peer.rescan()
            self.say(f"sharing {count} files")
        else:
            self.say(f"unknown command {safe_text(command)!r}; try 'help'")
        return True

    # ------------------------------------------------------------ commands --

    def banner(self) -> None:
        peer = self.peer
        self.say(f"openwire {__version__}  -  peer {peer.peer_id} ({peer.nick or 'no nick'})")
        self.say(f"  sharing    {len(peer.index)} files from {peer.share_dir}")
        self.say(f"  listening  {peer.host}:{peer.port}   downloads -> {peer.download_dir}")
        self.say(
            f"  tracker    {peer.tracker_addr[0]}:{peer.tracker_addr[1]}  (sees you as {safe_text(str(peer.observed_host))})"
        )
        self.say("Type 'help' for commands.")

    async def cmd_search(self, query: str) -> None:
        if not query:
            self.say("usage: search <words>")
            return
        self.results = await self.peer.search(query)
        if not self.results:
            self.say("no matches")
            return
        self.say(f"  {'#':>3}  {'size':>10}  {'srcs':>4}  name")
        for number, result in enumerate(self.results, start=1):
            self.say(f"  {number:>3}  {fmt_size(result.size):>10}  {result.total_sources:>4}  {safe_text(result.name)}")
        self.say("get <#> to download")

    async def cmd_get(self, arg: str) -> None:
        if normalize_sha256(arg):  # checked first: a hash of all digits is still a hash
            found = await self.peer.lookup(arg)
            if found is None:
                self.say("nobody is sharing that file right now")
                return
            result = found
        elif arg.isdigit() and 1 <= int(arg) <= len(self.results):
            result = self.results[int(arg) - 1]
        elif arg.isdigit():
            self.say(f"no result number {arg}; run a search first" if not self.results else f"pick 1 to {len(self.results)}")
            return
        else:
            self.say("usage: get <result number from the last search>  or  get <sha256>")
            return

        self.say(
            f"downloading {safe_text(result.name)} ({fmt_size(result.size)}) from {len(result.sources)} source(s)"
        )
        last_print = 0.0

        def progress(done: int, total: int) -> None:
            nonlocal last_print
            now = time.monotonic()
            if done == total or now - last_print >= 0.2:
                last_print = now
                pct = 100.0 if total == 0 else done * 100.0 / total
                print(f"\r  {pct:5.1f}%  {fmt_size(done)} / {fmt_size(total)}", end="", file=self.out, flush=True)

        try:
            saved = await self.peer.download(result, progress=progress)
        finally:
            self.say()
        self.say(f"saved to {safe_text(str(saved))}")

    def cmd_ls(self) -> None:
        if not self.peer.index:
            self.say(f"sharing nothing; put files in {self.peer.share_dir} and run 'rescan'")
            return
        for shared in sorted(self.peer.index.values(), key=lambda f: f.name.lower()):
            self.say(f"  {fmt_size(shared.size):>10}  {shared.sha256[:12]}  {safe_text(shared.name)}")
        self.say(f"{len(self.peer.index)} files")

    def cmd_status(self) -> None:
        peer = self.peer
        state = "connected" if peer.connected else "disconnected (reconnecting)"
        self.say(f"  peer id    {peer.peer_id}  nick {peer.nick or '-'}")
        self.say(f"  listening  {peer.host}:{peer.port}")
        self.say(f"  tracker    {peer.tracker_addr[0]}:{peer.tracker_addr[1]}  {state}")
        self.say(f"  seen as    {safe_text(str(peer.observed_host))}:{peer.port}")
        self.say(f"  sharing    {len(peer.index)} files from {peer.share_dir}")
        self.say(f"  downloads  {peer.download_dir}")
