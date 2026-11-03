# Distributed KV Store

A Java-based distributed key-value store developed as a hands-on exploration of concurrency, networking, durable storage, replication, consistency, and fault tolerance.

The project began as a concurrent, single-node TCP server and has evolved into a three-node replicated system with persistent storage, majority-based commits, and automatic follower recovery.

## Current Status

**V2 — Three-node leader-follower replication and recovery.**

The system currently supports:

- Three independently running Java server nodes
- Persistent, full-mesh TCP connections between nodes
- Static leader-follower architecture
- Client connections to any node
- Follower-to-leader forwarding of write operations
- Leader-ordered mutation replication
- Majority-based commit acknowledgement (2 of 3 nodes)
- Write-ahead logging (WAL) and durable operation storage
- Persistent snapshots and automatic checkpointing
- Recovery through snapshots and WAL replay
- Automatic follower catch-up using missing log entries or snapshot transfers
- Startup and reconnection synchronisation
- Read/write gating while followers are synchronising
- Concurrent TCP clients and per-key reader/writer coordination

### Current Limitations

This is an educational distributed systems implementation, not a production-ready database.

- The leader is statically assigned; automatic leader election and failover are not yet implemented.
- Writes cannot complete while the leader is unavailable.
- Follower reads are not guaranteed to be strictly linearizable.
- Network partitions, conflicting log histories, and arbitrary crash points require further correctness testing.
- Majority acknowledgements are implemented, but the system does not yet provide a complete consensus protocol.

## Architecture

### Cluster Topology

The cluster consists of three nodes:

| Node | Role | Client Port | Cluster Port |
|---|---|---|---|
| Melchior (A) | Leader | 7001 | 8001 |
| Balthasar (B) | Follower | 7002 | 8002 |
| Casper (C) | Follower | 7003 | 8003 |

Nodes maintain persistent TCP connections for inter-node communication. Clients can connect to any node, while the leader coordinates mutation ordering and replication.

### Mutation Processing

1. A client submits a mutation to any node.
2. A follower forwards the mutation to the leader, if necessary.
3. The leader assigns a log index and persists the operation.
4. The leader replicates the operation to followers.
5. A follower persists the operation and acknowledges replication.
6. The leader advances the commit index after receiving sufficient acknowledgement for a majority.
7. Committed operations are applied to the in-memory state.
8. The client receives the operation result.

If a write times out, its commit status may be unknown to the client.

### Persistence and Recovery

Each node maintains durable operation history using a write-ahead log and snapshots.

On startup, persisted state is reconstructed using the latest snapshot and subsequent committed log entries.

Checkpointing reduces the amount of log history required for recovery.

When a follower falls behind, the leader can transfer missing WAL entries or a snapshot followed by the required log tail.

### Cluster Synchronisation

Followers begin in a `SYNCING` state.

When a connection to the leader is established, the leader sends its committed log index through a `SYNC_STATUS` message.

The follower compares that index with its local replication state and performs catch-up if necessary.

Only after completing the required synchronisation does the follower transition to `READY`.

While synchronising, client reads and mutations are rejected with a retry response.

If the leader connection is lost, followers return to `SYNCING` until reconnection and synchronisation complete.

This prevents a rejoining follower from serving reads against state known to be outdated at the handshake boundary. It does not guarantee linearizable follower reads during ordinary replication.

## Testing

The project has undergone manual integration and regression testing covering:

- Cluster startup and inter-node connectivity
- Client operations through leaders and followers
- Cross-node replication
- Persistence across full cluster restarts
- Majority availability with one follower offline
- Write timeouts without a majority
- Follower disconnects and repeated reconnections
- WAL-based catch-up
- Snapshot recovery
- Automatic synchronisation after reconnection
- Leader disconnection and recovery
- Read and write readiness gating
- Concurrent writes to the same key

The October 8 regression campaign recorded 10 passing scenarios. A later campaign covered 26 numbered scenarios, with the final scenario's restart-persistence checks passing and its additional cross-node write check not run.

These are manually observed results rather than formal proofs of distributed consistency or exhaustive fault-injection testing.

See [Cluster Regression Test Report](docs/testing/cluster-regression-report.md).

## Documentation

- [Distributed Architecture](docs/distributed-architecture.md)
- [Multi-Node Operation Processing Audit](docs/multinode-operation-processing-audit.md)
- [Cluster Regression Test Report](docs/testing/cluster-regression-report.md)

## Version History

### V2 — Multi-Node Replication and Recovery

Extended the persistent single-node implementation into a three-node distributed system.

Added:

- Leader-follower replication
- Persistent cluster connections
- Follower write forwarding
- Leader-ordered operation logging
- Majority-based commits
- Replication indices and commit tracking
- Automatic follower catch-up
- Snapshot-based cluster recovery
- Connection lifecycle management
- Startup and reconnection synchronisation
- Client readiness gating
- Multi-node integration and fault-recovery testing

### V1 — Persistence and Checkpointing

Extended the single-node server with durable storage and crash recovery.

Added:

- Append-only mutation logging
- Ordered log replay
- Persistent snapshots
- Automatic checkpointing
- Background checkpoint worker
- Coordination between checkpoints and client operations
- Snapshot and log-tail recovery
- Crash-safe snapshot replacement
- Length-prefixed persistence encoding
- Storage durability improvements

### V0 — Concurrent In-Memory Server

Initial implementation focused on networking and concurrency.

Added:

- TCP client/server communication
- PUT, GET and DELETE operations
- Multiple concurrent client connections
- Per-key reader/writer locking
- Concurrent reads and exclusive writes

## Roadmap

### Completed

- [x] Concurrent single-node server
- [x] Persistent operation log
- [x] Crash recovery and WAL replay
- [x] State snapshots and automatic checkpointing
- [x] Persistence hardening
- [x] Three-node cluster networking
- [x] Static leader-follower replication
- [x] Majority-based commits
- [x] Follower forwarding
- [x] Automatic follower catch-up
- [x] Snapshot-based recovery
- [x] Startup and reconnection synchronisation
- [x] Multi-node regression testing

### Planned

- [ ] Post-V2 correctness audit and targeted refactoring
- [ ] Leader failure detection and election protocol
- [ ] Leader terms and voting
- [ ] Replicated-log consistency across leader changes
- [ ] Recovery of uncommitted entries after failover
- [ ] Split-brain prevention
- [ ] Stronger follower-read consistency guarantees
- [ ] Network partition and crash-injection testing
- [ ] Complete consensus protocol

## Project Goals

The primary objective is to understand distributed systems by implementing their underlying mechanisms rather than relying on existing distributed database frameworks.

Development prioritises correctness, durability, fault recovery, and understandable architecture over throughput or large-scale deployment.

The next major phase will investigate leader election and consensus, building on the existing three-node replication architecture.
