import argparse

import pytest

from openwire.cli import build_parser, parse_hostport


def test_parse_hostport():
    assert parse_hostport("10.0.0.5:9000", 8888) == ("10.0.0.5", 9000)
    assert parse_hostport("tracker.local", 8888) == ("tracker.local", 8888)
    assert parse_hostport(":9000", 8888) == ("127.0.0.1", 9000)
    for bad in ("host:abc", "host:0", "host:70000"):
        with pytest.raises(argparse.ArgumentTypeError):
            parse_hostport(bad, 8888)


def test_parser_defaults():
    args = build_parser().parse_args(["peer"])
    assert args.command == "peer"
    assert args.tracker == "127.0.0.1:8888"
    assert args.port == 6699
    assert str(args.share) == "shared"
    assert str(args.downloads) == "downloads"

    args = build_parser().parse_args(["tracker", "--port", "0", "--host", "0.0.0.0"])
    assert (args.command, args.port, args.host) == ("tracker", 0, "0.0.0.0")


def test_a_command_is_required():
    with pytest.raises(SystemExit):
        build_parser().parse_args([])
