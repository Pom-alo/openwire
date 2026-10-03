# openwire

A small peer-to-peer file sharing system written from scratch in Python, built to learn how the Napster and LimeWire generation of software actually worked underneath.

No frameworks and no dependencies beyond Python 3.11. The point is to see sockets, message framing, content hashing and peer discovery with nothing in the way.

**Status:** Phase 1, the Napster model, is complete and tested. The [roadmap](docs/ROADMAP.md) covers what comes next.

## How it works

```
                   +-----------+
   "who has X?"    |  tracker  |    "alice does"
        +--------->|  (index)  |<---------+
        |          +-----------+          |
        |   hello / register / search     |
        |                                 |
   +---------+     get <sha256>      +---------+
   |   bob   |---------------------->|  alice  |
   |  (peer) |<======================|  (peer) |
   +---------+     the file bytes    +---------+
```

Every peer keeps one connection open to the tracker and tells it which files it is sharing: a name, a size and the SHA-256 of the contents. Searches go to the tracker. The tracker never sees file contents and stores nothing on disk. When a peer disconnects, its files leave the index that instant.

The file itself travels directly from peer to peer over a fresh TCP connection. The downloader asks for a file by hash, verifies every byte against that hash when the transfer ends, and only then gives the file its name. This is the shape Napster had in 1999: a central index for discovery, direct connections for transfer.

Every message on the wire is described in [docs/PROTOCOL.md](docs/PROTOCOL.md).

## Quick start

Everything below runs on one machine. You need Python 3.11 or newer and three terminals.

Install in editable mode, which gives you the `openwire` command:

```
pip install -e .
```

Terminal 1, the tracker:

```
openwire tracker
```

Terminal 2, a peer called alice who shares a folder. Put a few files you own in it first:

```
mkdir -p demo/alice
openwire peer --nick alice --share demo/alice --downloads demo/alice-downloads
```

Terminal 3, a peer called bob with an empty share folder. Two peers on one machine need different ports:

```
mkdir -p demo/bob
openwire peer --nick bob --share demo/bob --downloads demo/bob-downloads --port 6700
```

In bob's shell:

```
openwire> search <a word from one of alice's file names>
    #        size  srcs  name
    1     3.2 MiB     1  some file.mp3
get <#> to download
openwire> get 1
downloading some file.mp3 (3.2 MiB) from 1 source(s)
  100.0%  3.2 MiB / 3.2 MiB
saved to demo/bob-downloads/some file.mp3
```

Quit alice and search again from bob: the file is gone from the results, because the index only knows about connected peers.

`python -m openwire ...` works too if you would rather not install anything.

### Across a LAN

Run the tracker with `--host 0.0.0.0`, and each peer with `--host 0.0.0.0 --tracker <tracker-ip>:8888`. The tracker records the address each peer connects from, so peers find each other without configuration. If a peer has several network interfaces and the tracker's view of it is wrong, `--advertise-host` overrides it. Nothing here does NAT traversal, so it stays within one network. That is a later phase's problem.

## Peer commands

| command | what it does |
|---|---|
| `search <words>` | ask the tracker for files whose names contain every word (case-insensitive) |
| `get <#>` | download result `#` from the last search; tries each source in turn |
| `get <sha256>` | download any file by hash, from whoever has it right now |
| `ls` | list what you are sharing |
| `status` | your peer id, address, tracker connection |
| `rescan` | re-hash the share folder and re-register |
| `help` | list the commands |
| `quit` (or `exit`, `q`) | leave; your files vanish from the index immediately |

Downloads resume: a partial file is kept in `downloads/.partial/` and the next `get` asks only for the rest.

## What Phase 1 teaches

- **Framing.** TCP is a byte stream. Every protocol must mark where messages end. openwire uses a 4-byte length prefix and JSON bodies. See `openwire/protocol.py`.
- **Content addressing.** Files are identified by SHA-256, not by name, so one file shared under three names is one search result, and a downloader can prove it got what it asked for. See `openwire/hashing.py`.
- **A centralised index and its liveness model.** The tracker holds everything in memory and drops a peer's files the moment its connection closes. Simple, instant, and a single point of failure. See `openwire/tracker.py`.
- **Direct transfers with an untrusting receiver.** The downloader sanitises the file name, checks the size against what the tracker said, and verifies the hash before the file gets its real name. The seeder serves only by hash, so there is no path to traverse. See `openwire/transfer.py`.
- **Keeping a long-lived connection alive.** Heartbeats, reconnection with backoff, re-registering after a reconnect. See `openwire/peer.py`.

## Project layout

```
openwire/
  protocol.py   length-prefixed JSON framing, shared by everything
  hashing.py    SHA-256 of files, scanning a share folder
  tracker.py    the index server (hello / register / search / lookup / ping)
  transfer.py   serving a file by hash; downloading and verifying one
  peer.py       a peer: file server + tracker link + downloads
  shell.py      the interactive command line
  cli.py        the `openwire tracker` and `openwire peer` commands
tests/          pytest suite; runs a tracker and peers on the loopback interface
docs/           PROTOCOL.md (the wire format) and ROADMAP.md (phases 2 to 5)
```

## Tests

```
pip install -e ".[dev]"
pytest
```

The suite starts a real tracker and real peers on 127.0.0.1 with random ports, so nothing else on the machine is touched.

## Scope

This is a study project. It runs between peers you point at each other on your own machine or network. There is no bootstrap list and it joins no public network. Share files you have the right to share.

## License

MIT. See [LICENSE](LICENSE).
