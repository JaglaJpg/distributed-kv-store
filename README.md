# Distributed KV Store

A Java key-value store built as a hands-on exploration of concurrency,
networking, persistence, replication, and distributed systems.

## Current Status

V0 — Single-node, in-memory key-value server.

Currently supports:
- PUT <key> <value>
- GET <key>
- DELETE <key>
- Multiple concurrent TCP clients
- Per-key reader/writer locking
- Concurrent reads on the same key
- Exclusive writes

## Architecture

Client
  |
  | TCP
  v
KV Server
  |
  +-- Client Handler Thread
  +-- Client Handler Thread
  +-- Client Handler Thread
           |
           v
      Shared KV Store

Each connected client is handled by its own thread. The shared store uses
per-key reader/writer coordination to allow concurrent reads while keeping
writes exclusive.

## Roadmap

- Persistent local storage
- Multi-node operation
- Replication
- Failure handling and recovery
- Leader election
- Replicated command log
- Consensus
