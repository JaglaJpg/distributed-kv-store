# Distributed KV Store

A Java key-value store built as a hands-on exploration of concurrency, networking, persistence, replication, and distributed systems.

## Current Status

**V1 — Concurrent, persistent single-node key-value server.**

The server currently supports:

- PUT, GET, and DELETE operations
- Multiple concurrent TCP clients
- Per-key reader/writer locking
- Concurrent reads and exclusive writes
- Append-only operation logging
- Crash recovery through log replay
- Persistent state snapshots
- Automatic checkpointing after a mutation threshold
- Background checkpoint processing
- Coordination between client operations and checkpoints
- Snapshot + log-tail recovery
- Length-prefixed persistence encoding for keys and values

## Architecture

Each connected client is handled by its own thread and operates against shared key-value state.

Normal operations use per-key reader/writer coordination, allowing concurrent reads while keeping writes exclusive.

Mutations are recorded in an append-only operation log before being applied to the in-memory state. On startup, the server restores the latest snapshot and replays any operations remaining in the log.

A background worker periodically checkpoints the current state. During a checkpoint, new client operations are temporarily blocked while the worker takes exclusive access, writes a new snapshot, and truncates the operation log.

More detailed architecture documentation and diagrams will be added as the system evolves toward multi-node operation.

## Version History

### V1 — Persistence and Checkpointing

Extended the single-node server with persistent storage and crash recovery.

Added:

- Append-only mutation logging
- Recovery through ordered log replay
- Persistent state snapshots
- Automatic checkpointing based on mutation count
- Dedicated background checkpoint worker
- Global coordination between checkpoints and client operations
- Snapshot + log-tail recovery
- Safe snapshot replacement using a temporary file and atomic move
- Length-prefixed persistence encoding

### V0 — Concurrent In-Memory Server

Initial single-node implementation focused on networking and concurrency.

Added:

- PUT, GET, and DELETE operations
- TCP client/server communication
- Multiple concurrent client connections
- Per-key reader/writer locking
- Concurrent reads on the same key
- Exclusive writes

## Roadmap

- [x] Concurrent single-node server
- [x] Persistent operation log
- [x] Crash recovery through log replay
- [x] State snapshots
- [x] Automatic checkpointing
- [ ] Persistence hardening and crash-edge handling
- [ ] Multi-node operation
- [ ] Replication
- [ ] Failure handling and recovery
- [ ] Leader election
- [ ] Replicated command log
- [ ] Consensus
