# openwire wire protocol, version 1

This is the complete description of what goes over the network in Phase 1. With this document you could write a compatible peer or tracker in any language.

## 1. Framing

All control traffic travels in **frames**. A frame is a 4-byte big-endian unsigned length followed by exactly that many bytes of UTF-8 encoded JSON.

```
offset  size  content
0       4     N, the body length, big-endian unsigned integer
4       N     the body: one JSON object
```

Rules:

- The body must be a JSON object with a string field `type`.
- N must not exceed 1,048,576 (1 MiB). A receiver that sees a larger length closes the connection without reading the body.
- A receiver that cannot parse the body, or gets an object without a string `type`, closes the connection.
- Field order inside the JSON does not matter. Unknown fields are ignored.

Why a length prefix: TCP delivers an ordered stream of bytes and nothing more. One `read()` can return half a message or several messages run together. The sender states the length up front so the receiver knows exactly how many bytes to collect before parsing.

Example: the frame carrying `{"type":"ping"}` is 19 bytes.

```
00 00 00 0f                                       length = 15
7b 22 74 79 70 65 22 3a 22 70 69 6e 67 22 7d      {"type":"ping"}
```

## 2. Connection model

There are two kinds of connection.

**Peer to tracker.** Each peer keeps **one** long-lived TCP connection to the tracker. Traffic on it is strict request and response: the peer sends one message and waits for exactly one reply before sending the next. The tracker remembers a peer only while this connection is open. When it closes, for any reason, the peer and all its files are removed from the index.

**Peer to peer.** Each file transfer uses a **fresh** TCP connection from the downloader to the seeder. It carries one request, one reply header, the raw bytes of the file, and then closes.

Hosts and ports: the tracker listens on 8888 by default and peers accept transfers on 6699 by default. These are the ports Napster used for its servers and clients respectively, and they are only defaults.

## 3. Tracker messages

The first message on a new tracker connection must be `hello`. Anything else gets an `error` with code `hello_required` and the connection is closed.

### hello / welcome

```json
{"type": "hello", "version": 1, "port": 6699, "nick": "alice", "host": "10.0.0.7"}
```

| field | required | meaning |
|---|---|---|
| `version` | no | protocol version the peer speaks; informational in v1 |
| `port` | yes | the TCP port this peer accepts transfers on (1 to 65535) |
| `nick` | no | display name, printable, at most 32 characters; defaults to the peer id |
| `host` | no | the address other peers should connect to; defaults to the address the tracker sees this connection coming from |

Reply:

```json
{"type": "welcome", "version": 1, "peer_id": "3fa9c2d41b7e", "your_host": "10.0.0.7"}
```

`peer_id` is assigned by the tracker and is new on every connection. `your_host` is the address the tracker will hand to other peers.

### register / ok

Replaces the peer's entire file list. Sent right after `welcome` and again whenever the shared folder changes.

```json
{"type": "register", "files": [
  {"name": "Albums/Live/track01.mp3", "size": 3345921, "sha256": "9f86d08..."},
  {"name": "notes.txt", "size": 812, "sha256": "60303ae..."}
]}
```

Each entry needs a `name` (1 to 512 characters, any text), a `size` (0 to 2^53) and a `sha256` (64 hex characters, case-insensitive). At most 50,000 entries. If any entry is invalid, the whole message is rejected with `bad_request` and the previous list stays in force.

Reply:

```json
{"type": "ok", "count": 2}
```

### search / results

```json
{"type": "search", "query": "blue train"}
```

The query is split on whitespace; a file matches if every word appears in its name, ignoring case. The query must be 1 to 200 characters and contain at least one word. The requesting peer's own files are never returned.

Reply:

```json
{"type": "results", "query": "blue train", "total": 1, "results": [
  {"sha256": "9f86d08...", "name": "Music/Blue Train.mp3", "size": 54321123, "sources": [
    {"peer_id": "3fa9c2d41b7e", "nick": "alice", "host": "10.0.0.7", "port": 6699},
    {"peer_id": "b81c0f52e9a0", "nick": "bob",   "host": "10.0.0.9", "port": 6700}
  ]}
]}
```

Results are grouped by content hash: one entry per distinct file, listing every peer that has it. `name` and `size` come from whichever peer registered the file first. Entries are ordered by number of sources, then by name. `total` is the number of matches; `results` holds at most 50 of them.

### lookup / sources

Who has this exact file right now? Used to download by hash and to find fresh sources for a retry.

```json
{"type": "lookup", "sha256": "9f86d08..."}
```

Reply:

```json
{"type": "sources", "sha256": "9f86d08...", "name": "Music/Blue Train.mp3", "size": 54321123, "sources": [...]}
```

If nobody has the file, `sources` is an empty list and `name` and `size` are `null`.

### ping / pong

```json
{"type": "ping"}
{"type": "pong"}
```

Peers send a ping every 30 seconds. The tracker drops any connection that is silent for 90 seconds.

### bye

```json
{"type": "bye"}
```

No reply. The tracker closes the connection and withdraws the peer's files. Simply closing the socket has the same effect; `bye` is just polite.

### error

Any request can be answered with an error instead of its normal reply. The connection stays open unless noted.

```json
{"type": "error", "code": "bad_request", "message": "query is empty"}
```

| code | when |
|---|---|
| `hello_required` | the first message was not `hello`; connection is closed |
| `bad_request` | a field is missing, the wrong type, or out of range |
| `unknown_type` | the `type` is not one the tracker knows |

## 4. Transfer messages

The downloader opens a TCP connection to a source's `host` and `port` and sends one frame:

```json
{"type": "get", "sha256": "9f86d08...", "offset": 0}
```

`offset` (default 0) is how many bytes the downloader already has. It must be between 0 and the file's size inclusive.

The seeder answers with one frame, then the raw bytes.

```json
{"type": "file", "sha256": "9f86d08...", "name": "Albums/Live/track01.mp3", "size": 3345921}
```

Immediately after that frame the seeder writes `size - offset` raw bytes, the file's contents from `offset` to the end, with no further framing. Then it closes the connection. The seeder must wait for a `get` for at most 15 seconds.

Or the seeder answers with an error frame and closes:

| code | when |
|---|---|
| `bad_request` | not a `get`, or `sha256` or `offset` is malformed |
| `not_found` | the seeder is not sharing a file with that hash |
| `bad_offset` | `offset` is larger than the file |
| `unavailable` | the file is in the index but can no longer be opened |

### What the downloader checks

1. The reply `sha256` equals the one requested.
2. The reply `size` equals the size the tracker reported. A disagreement aborts the transfer.
3. After the last byte, the SHA-256 of the whole file (bytes already on disk plus bytes just received) equals the requested hash. On a mismatch the partial file is deleted.
4. The `name` is reduced to a bare filename before use: directory parts are dropped, characters that are illegal on Windows become `_`, leading and trailing dots and spaces are removed (so a download is never a hidden file), reserved device names get a leading underscore, and if nothing remains the hash prefix is used.

### Resume

The downloader keeps an incomplete file at `<downloads>/.partial/<sha256>.part`. On the next attempt it hashes what it has, sends its length as `offset`, and appends. If the partial file is longer than the expected size it is discarded and the download starts over.

Known limitation: if a dishonest seeder sends wrong bytes and then disconnects before the end, the partial file is poisoned and the next attempt, even from an honest peer, will fail the final hash check and discard it. Per-piece hashes in Phase 2 remove this problem.

## 5. Limits

| limit | value |
|---|---|
| frame body | 1 MiB |
| files per register | 50,000 |
| file name | 512 characters |
| nick | 32 printable characters |
| query | 200 characters, at least one word |
| results per search | 50 |
| tracker idle timeout | 90 s (peers ping every 30 s) |
| seeder wait for `get` | 15 s |
| downloader connect / read timeout | 10 s / 30 s |

## 6. Trust boundaries

- The tracker trusts nothing in a message beyond its shape. Every field is type- and range-checked.
- The tracker's view of a peer's address wins over what the peer claims, unless the peer sends an explicit `host`.
- A downloader trusts nothing from a seeder: name, size and bytes are all checked.
- A seeder exposes only what it has indexed, addressed only by hash. There is no way to ask it for a path.
- Nothing is encrypted or authenticated. Anyone on the network path can read the traffic, and a peer id proves nothing. This is faithful to the 1999 originals and is on the roadmap.

## 7. How this differs from Napster

| | Napster (1999) | openwire v1 |
|---|---|---|
| framing | 2-byte little-endian length, 2-byte message type, ASCII payload | 4-byte big-endian length, JSON object with `type` |
| file identity | file name and size | SHA-256 of the contents |
| search results | one line per peer per file | grouped by hash, one entry per file with all sources |
| transfer request | `GET` + nick + filename + offset, sent as plain text | `get` frame with hash and offset |
| verification | none | SHA-256 over the whole file |
| server state | in memory, per connection | in memory, per connection |
