# Roadmap

openwire follows the order history took. Each phase exists because the one before it had a problem that real systems ran into, and each adds one big idea.

## Phase 1: the Napster model (done)

A central tracker holds the index; file bytes move directly between peers. Built and tested. See [PROTOCOL.md](PROTOCOL.md).

Problems it leaves open, in the order the next phases address them:

- a file comes from exactly one peer at a time, and if that peer leaves mid-transfer you start over with someone else;
- a dishonest peer can poison a partial download;
- the tracker is a single point of failure and control;
- nothing works across NAT, and nothing is encrypted.

## Phase 2: pieces and swarming (the BitTorrent idea)

Split every file into fixed-size pieces (256 KiB, say) and publish the hash of every piece alongside the hash of the whole. Then:

- **Verify as you go.** Each piece is checked on arrival. A bad piece is thrown away and re-requested; the rest of the download is unaffected. This fixes the poisoned-partial problem.
- **Download from everyone at once.** Ask different sources for different pieces in parallel. Throughput adds up instead of being capped by the slowest single peer.
- **Share while downloading.** As soon as a piece is verified you can serve it. A popular file gets *faster* as more people want it, which is the opposite of every client-server system.
- **Rarest first.** Pick the piece the fewest sources have, so the swarm as a whole never loses the last copy of anything.

Protocol changes: `register` carries a piece size and a piece-hash list per file (or a hash of that list, with the list fetched from a peer); `get` asks for a piece index or byte range instead of an offset; a `have` bitfield tells a downloader which pieces a source can serve. Napster never did any of this. LimeWire added "swarming" in the early 2000s, and BitTorrent was built around it from day one.

Smaller things that belong here: downloads running in the background while the shell stays usable; a persisted hash cache so `rescan` does not re-read unchanged files; a download queue.

## Phase 3: no tracker at all (the Gnutella model, which LimeWire ran)

Delete the tracker. Every peer connects to a handful of other peers, and a search is a message that **floods**: each peer forwards it to its neighbours, who forward it to theirs, until a hop limit (TTL) is reached. A peer that has a match sends a `QueryHit` back along the path the query arrived on.

What you have to build to make that work:

- `Ping` and `Pong` to discover more peers, and a way to join at all (a cached list of peers you saw last time; the original clients shipped with a hard-coded list of hosts).
- A **routing table** keyed by message GUID, so a `QueryHit` can be routed back hop by hop to the peer that asked, without anyone knowing the full path.
- **Duplicate suppression**: a peer that sees the same GUID twice drops it, or the flood never ends.
- TTL and hop counters, with caps, so a query cannot circulate forever or reach the whole network.

Then watch it struggle. Every search touches every peer within TTL hops; traffic grows with the square of the network, and modem-era peers drowned in it. Gnutella's answer was **ultrapeers**: well-connected nodes that hold the index for many leaf nodes and route between themselves, which is a partial walk back toward a central index. Building that in is the natural end of Phase 3.

## Phase 4: a distributed hash table (Kademlia)

What modern systems do instead of flooding: every peer and every file gets a 160-bit id, and the peers whose ids are closest (by XOR distance) to a file's hash are responsible for knowing who has it. Finding a file takes about log2(N) hops, with no central server and no flooding.

To build: node ids and XOR distance, k-buckets (the routing table), and four messages: `PING`, `FIND_NODE`, `FIND_VALUE`, `STORE`. Then republishing and expiry so stale entries disappear. This is the DHT behind BitTorrent's trackerless mode and the basis of IPFS.

## Phase 5: the real world

Things every phase above quietly assumed away:

- **NAT traversal.** Most peers are behind a home router and cannot accept inbound connections. UPnP port mapping, STUN to learn your public address, and hole punching or a relay for the hard cases.
- **Encryption and identity.** TLS or Noise between peers, and public keys as peer ids so that an id actually proves something.
- **Incentives.** Why would anyone upload? BitTorrent's tit-for-tat choking algorithm is the classic answer.
- A graphical client, if you want one, since the whole point of the originals was that your parents could use them.

## Exercises along the way

Smaller things worth doing at any point, roughly in order of effort:

1. Make the tracker log a one-line summary (peers, files) every minute.
2. Add `search` operators: `size>10mb`, `ext:mp3`.
3. Make the shell's `get` run in the background and add a `downloads` command showing progress.
4. Persist the hash index to a JSON file keyed by path, size and mtime, so startup is instant.
5. Write a second peer implementation in another language against PROTOCOL.md, and make the tests pass against it.
6. Capture a session with Wireshark and walk through the frames by hand.
7. Measure: how many searches per second can one tracker answer with 1,000 peers and 100,000 files? Where is the time going?
