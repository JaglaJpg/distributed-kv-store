# Raft Replication Refactor — Handover Notes

**Status:** Existing replication implementation paused  
**Working branch:** `feature/raft-consensus`  
**Refactor branch:** `refactor/append-entries`

## 1. Why We're Refactoring

The initial Raft-lite implementation introduced separate protocol messages for replication, acknowledgements, rejection handling, commits and heartbeats.

Although functional pieces of conflict detection and repair have been implemented, coordinating these separate messages is becoming unnecessarily complex.

The decision is to align the replication protocol more closely with standard Raft's **AppendEntries RPC**, reducing special-case handling and making the implementation easier to reason about, test and maintain.

## 2. Current Implementation

The existing implementation includes:

- Leader elections and term tracking.
- Leader-generated `NO_OP` entries.
- Replication with previous-entry term validation.
- Conflict rejection and backwards retry.
- Truncation and replacement of conflicting uncommitted entries.
- ACK-driven forward replication.
- Majority-based commit handling.

**Important:** These mechanisms have not yet been fully integrated and validated as a complete Raft consensus protocol.

## 3. Planned Refactor

Replace the existing `REPLICATE`, `REPLICATE_ACK`, `REPLICATE_REJECT`, `COMMIT` and heartbeat coordination with a unified AppendEntries request/response protocol.

AppendEntries will handle:

1. Leader heartbeats and authority.
2. Log consistency checks and replication.
3. Propagation of the leader's commit index.

Introduce per-follower, in-memory replication tracking:

- `nextIndex` — next entry to attempt to replicate.
- `matchIndex` — highest confirmed matching log index.

## 4. Scope

**Primary refactor:** `ClusterManager`

**Expected minor changes:** Replication state management, operation-processing interfaces and protocol helpers.

**Largely retained:** WAL persistence, snapshots, state machine, KV operations and existing election infrastructure.

Snapshot-boundary handling, crash consistency and commit safety still require verification.

## 5. Implementation Plan

- [ ] Define AppendEntries request and response formats.
- [ ] Implement follower-side AppendEntries processing.
- [ ] Implement leader-side `nextIndex` and `matchIndex` tracking.
- [ ] Integrate majority-based commit advancement.
- [ ] Integrate heartbeat-based commit propagation.
- [ ] Handle snapshot boundaries and follower catch-up.
- [ ] Remove obsolete protocol handlers.
- [ ] Run conflict-repair, failover and recovery tests.

## 6. Git Strategy

1. Commit the existing implementation on `feature/raft-consensus`.
2. Create `refactor/append-entries` from that branch.
3. Implement and validate the new protocol.
4. Merge the refactor into `feature/raft-consensus`.
5. Complete consensus integration and testing.
6. Merge the completed consensus work into `main`.

## 7. Guiding Principle

Follow an incremental, learning-driven implementation of Raft.

**Understand → Design → Implement → Verify**

Prioritise correctness and clarity over optimisations. Preserve working storage and state-machine components wherever possible.

The goal is not to restart the project, but to replace an increasingly complex coordination layer with a well-defined consensus protocol.