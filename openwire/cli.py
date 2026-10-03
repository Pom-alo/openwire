"""Command-line entry points: ``openwire tracker`` and ``openwire peer``."""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path

from . import __version__
from .peer import DEFAULT_PORT as PEER_PORT
from .peer import Peer, TrackerError, TrackerUnreachable
from .shell import Shell
from .tracker import DEFAULT_PORT as TRACKER_PORT
from .tracker import Tracker


def port_number(text: str) -> int:
    """argparse type for a listening port: 0 (pick one) to 65535."""
    try:
        port = int(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"{text!r} is not a number") from None
    if not 0 <= port <= 65535:
        raise argparse.ArgumentTypeError(f"port {port} is out of range (0 to 65535)")
    return port


def parse_hostport(value: str, default_port: int) -> tuple[str, int]:
    """'host:port' -> (host, port). A bare host gets the default port.

    IPv6 addresses go in brackets, as in URLs: ``[::1]:8888``.
    """
    if value.startswith("["):
        end = value.find("]")
        if end == -1:
            raise argparse.ArgumentTypeError(f"unclosed '[' in {value!r}")
        host, rest = value[1:end], value[end + 1 :]
        if not rest:
            return host, default_port
        if not rest.startswith(":"):
            raise argparse.ArgumentTypeError(f"expected ':port' after ']' in {value!r}")
        port_text = rest[1:]
    else:
        host, sep, port_text = value.rpartition(":")
        if not sep:
            return value or "127.0.0.1", default_port
    try:
        port = int(port_text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"bad port in {value!r}") from None
    if not 1 <= port <= 65535:
        raise argparse.ArgumentTypeError(f"port out of range in {value!r}")
    return host or "127.0.0.1", port


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="openwire",
        description="A small peer-to-peer file sharing system, built to learn how P2P works.",
    )
    parser.add_argument("--version", action="version", version=f"openwire {__version__}")
    sub = parser.add_subparsers(dest="command", required=True, metavar="COMMAND")

    tracker = sub.add_parser("tracker", help="run an index server that peers register with and search")
    tracker.add_argument("--host", default="127.0.0.1", help="address to listen on (default 127.0.0.1; 0.0.0.0 for your LAN)")
    tracker.add_argument("--port", type=port_number, default=TRACKER_PORT, help=f"port to listen on (default {TRACKER_PORT})")
    tracker.add_argument("-v", "--verbose", action="store_true", help="debug logging")

    peer = sub.add_parser("peer", help="share a folder and search/download through a tracker")
    peer.add_argument("--tracker", default=f"127.0.0.1:{TRACKER_PORT}", metavar="HOST:PORT", help="tracker to join (default 127.0.0.1:%d)" % TRACKER_PORT)
    peer.add_argument("--share", type=Path, default=Path("shared"), metavar="DIR", help="folder to share (default ./shared)")
    peer.add_argument("--downloads", type=Path, default=Path("downloads"), metavar="DIR", help="where downloads are saved (default ./downloads)")
    peer.add_argument("--host", default="127.0.0.1", help="address to accept transfers on (default 127.0.0.1; 0.0.0.0 for your LAN)")
    peer.add_argument("--port", type=port_number, default=PEER_PORT, help=f"port to accept transfers on (default {PEER_PORT}; 0 picks a free one)")
    peer.add_argument("--nick", help="a name other peers see next to your files")
    peer.add_argument("--advertise-host", metavar="HOST", help="address other peers should connect to, if the tracker's view of you is wrong")
    peer.add_argument("-v", "--verbose", action="store_true", help="debug logging")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    # File names from other peers can hold characters the console's code page
    # cannot show. Print a placeholder rather than dying with UnicodeEncodeError.
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(errors="replace")
    if args.verbose:
        level = logging.DEBUG
    else:
        level = logging.INFO if args.command == "tracker" else logging.WARNING
    logging.basicConfig(
        level=level, stream=sys.stderr, datefmt="%H:%M:%S", format="%(asctime)s %(levelname)-7s %(name)s: %(message)s"
    )
    try:
        if args.command == "tracker":
            return asyncio.run(run_tracker(args))
        return asyncio.run(run_peer(args))
    except KeyboardInterrupt:
        print(file=sys.stderr)
        return 0


async def run_tracker(args: argparse.Namespace) -> int:
    tracker = Tracker(args.host, args.port)
    try:
        await tracker.start()
    except OSError as exc:
        print(f"cannot listen on {args.host}:{args.port}: {exc}", file=sys.stderr)
        return 1
    print(f"openwire tracker {__version__} listening on {tracker.host}:{tracker.port}  (Ctrl-C to stop)", flush=True)
    try:
        await tracker.serve_forever()
    finally:
        await tracker.close()
    return 0


async def run_peer(args: argparse.Namespace) -> int:
    try:
        tracker_host, tracker_port = parse_hostport(args.tracker, TRACKER_PORT)
    except argparse.ArgumentTypeError as exc:
        print(f"--tracker: {exc}", file=sys.stderr)
        return 2
    if not args.share.is_dir():
        print(f"share folder {args.share} does not exist; create it (it may be empty)", file=sys.stderr)
        return 1

    peer = Peer(
        tracker=(tracker_host, tracker_port),
        share_dir=args.share,
        download_dir=args.downloads,
        host=args.host,
        port=args.port,
        nick=args.nick,
        advertise_host=args.advertise_host,
    )
    print(f"joining tracker {tracker_host}:{tracker_port} and indexing {args.share} ...", file=sys.stderr, flush=True)
    try:
        await peer.start()
    except TrackerUnreachable as exc:
        print(f"{exc}\nIs a tracker running? Start one in another terminal with:  openwire tracker", file=sys.stderr)
        await peer.close()
        return 1
    except TrackerError as exc:
        print(f"the tracker turned us away: {exc}", file=sys.stderr)
        await peer.close()
        return 1
    except OSError as exc:
        print(f"cannot listen on {args.host}:{args.port}: {exc}\nPick another port with --port.", file=sys.stderr)
        await peer.close()
        return 1

    try:
        await Shell(peer).run()
    finally:
        await peer.close()
    return 0
