# Cluster Regression and Recovery Test Report

**Test campaign:** September–November 2026  
**Latest testing:** 2026-11-03  
**System:** Three-node Java distributed key-value store  
**Nodes:** Melchior (static leader), Balthasar and Casper (followers)  
**Method:** Manual client-driven integration, regression and fault-recovery testing

## Summary

The October 8 regression campaign recorded **10/10 PASS**. A subsequent manual campaign covered **26 numbered scenarios**: **25 fully passed** and **one partially completed** (T26). The latter verified persistence after a full restart but did not execute the additional proposed cross-node PUT verification. The two campaigns overlap and should **not** be added together as 36 independent tests.

Results are based on observed client responses during manual testing, not automated assertions, packet captures, systematic fault injection or proof of linearizability.

## October 8 Baseline Regression Campaign

| ID | Scenario | Observed result | Status |
|---|---|---|---|
| B01 | Cluster startup | Earlier-started nodes retried connections at approximately two-second intervals; cluster connected when all nodes started. | PASS |
| B02 | Basic PUT/GET/DELETE | Operations worked through clients connected to any node. | PASS |
| B03 | Cross-node consistency | Write through one node, read through another, delete through a third. | PASS |
| B04 | Follower disconnect | Predecessor retried and successor detected disconnect while listening. | PASS |
| B05 | Follower reconnection | Restarted follower re-established peer connections. | PASS |
| B06 | Missed-operation catch-up | Follower recovered after a new post-reconnection operation. | PASS |
| B07 | Snapshot catch-up | Snapshot and subsequent WAL entries recovered across checkpoint boundary. | PASS |
| B08 | Persistence across restart | Persisted data survived restart. | PASS |
| B09 | Repeated follower disconnects | Multiple cycles reconnected. | PASS |
| B10 | Post-recovery replication | New operations replicated after recovery. | PASS |

RESET was also exercised but lacked recorded steps, so is not counted as a separate test.

## Later 26-Scenario Manual Test Campaign

| ID | Category | Scenario / action | Observed result | Status |
|---|---|---|---|---|
| T01 | Integration | Start three-node cluster; PUT `mergecheck01 hello` via Casper. | Initial forwarded write succeeded. | PASS |
| T02 | Regression | GET `mergecheck01` via Casper. | Returned `hello`. | PASS |
| T03 | Replication | GET same key on all three nodes. | Consistent value returned. | PASS |
| T04 | Replication | Overwrite key via Casper with `updated`. | Write succeeded. | PASS |
| T05 | Replication | GET overwritten key on all nodes. | All returned updated value. | PASS |
| T06 | Durability | Restart entire cluster; read key. | Value persisted. | PASS |
| T07 | Quorum | Stop Casper; PUT `quorumtest01 alive` through Melchior. | Write completed with two nodes available. | PASS |
| T08 | Recovery | Balthasar misses a write while offline, then rejoins. | Caught up only when later traffic triggered gap detection; exposed idle stale-read window. | PASS (old behaviour; defect identified) |
| T09 | Forwarding | PUT `quorumtest02 forwarded` via Casper while Balthasar offline. | Follower forwarding and quorum worked. | PASS |
| T10 | Recovery | Restart Balthasar; issue `quorumtest03 trigger`. | Missing entries caught up after triggering write. | PASS |
| T11 | Quorum failure | Stop both followers; PUT `noquorum01 shouldfail` on Melchior. | Client received timeout / commit status unknown. | PASS |
| T12 | Quorum restoration | Restart Balthasar; PUT `quorumrestored01 success`. | Quorum resumed without restarting leader. | PASS |
| T13 | Timeout semantics | Read earlier timed-out `noquorum01` on Melchior. | Value subsequently visible; timeout did not prove failure. | PASS |
| T14 | Replication | Read `noquorum01` on Balthasar. | Value visible after quorum restoration. | PASS |
| T15 | Recovery | Restart Casper after extended downtime; trigger catch-up using `catchuptrigger01 hello`. | Casper recovered missed values. | PASS |
| T16 | Delete replication | DELETE `mergecheck01` via Casper; read all nodes. | Deletion propagated. | PASS |
| T17 | Delete durability | Restart all nodes after deletion. | Deleted key remained absent. | PASS |
| T18 | Leader failure | Stop Melchior; PUT `leaderdown01 test` via Casper. | Timed out with unknown commit status under static-leader design. | PASS |
| T19 | Reconnection | Restart Melchior; retry writes. | Connections and writes recovered. | PASS |
| T20 | Connection lifecycle | Disconnect/reconnect Casper twice; exercise PUT/GET. | Operations recovered each cycle. | PASS |
| T21 | Forwarding regression | Multiple PUTs through Casper after reconnection; GET via Balthasar. | Correct values returned. | PASS |
| T22 | Concurrency | Two clients concurrently PUT `concurrent01` with `alpha` / `beta`. | All three nodes agreed on final value; winner not prescribed. | PASS |
| T23 | Handshake recovery | Stop Balthasar; write `handshaketest01 hello` while offline; restart Balthasar; GET without a new write. | Balthasar immediately caught up and returned `hello`. | PASS |
| T24 | Readiness lifecycle | Stop Melchior; GET through Balthasar; restart leader; GET again. | During outage returned `NOT_READY`; after recovery returned `hello`. | PASS |
| T25 | Mutation gate | Stop Melchior; PUT via Balthasar. | Immediately returned `NOT_READY` / retry shortly rather than waiting for timeout. | PASS |
| T26 | Restart regression | Restart all nodes; GET existing `handshaketest01` across cluster; proposed additional PUT `finalregression01 success` via Casper and GET everywhere. | Existing persisted key retrieved on all three nodes. Additional PUT/read-back portion not executed. | PARTIAL |

**Later campaign:** 25 PASS, 1 PARTIAL, 0 reported FAIL in final observed outcomes. During development, T08 revealed a stale-read-on-rejoin defect that motivated the later handshake fix; it should not be interpreted as proof that the earlier implementation was free of defects.

## Handshake Fix and Verification

The original follower catch-up was reactive: an offline follower could reconnect with stale committed state and remain behind until another replication or commit message exposed the gap. The subsequent implementation introduced:

- `SYNC_STATUS <leaderCommitIndex>` from the leader during connection setup.
- Follower `SYNCING` state until the leader's advertised committed boundary has been applied.
- Automatic catch-up requests on reconnect, even without new writes.
- `NOT_READY` gating for client GET and mutations while synchronising.
- Reset to `SYNCING` when the current leader connection is removed; readiness restored after handshake/catch-up, not merely on HELLO.

T23–T25 directly verified the intended recovery and gating behaviours. T26 verified persisted reads after full restart.

## Known Limitations and Untested Conditions

- **Static leader:** Leader election and automatic failover are not implemented. Writes cannot progress with the leader offline.
- **Read consistency:** The reconnect handshake removes the identified *idle stale-read window*, but follower GETs are not proven strictly linearizable during ordinary replication; a follower may briefly lag a newly committed write.
- **Quorum semantics:** A timeout means commit status is unknown, not necessarily that a write was rejected. T11–T14 demonstrated a timed-out write later becoming visible.
- **Concurrency coverage:** T22 tested two competing writes to one key, not sustained concurrent workloads, arbitrary interleavings or comprehensive linearizability histories.
- **Failure injection:** No systematic tests of crash-at-every-WAL-boundary, disk-full/fsync failure, torn writes, network partitions, split brain, or crashes mid-snapshot installation were recorded.
- **Log divergence and overlapping catch-up:** Not exhaustively tested. A further correctness audit is appropriate before leader election work.
- **T26 partial:** The new post-restart PUT through Casper and cross-node read-back was not run.

## Assessment

The three-node implementation passed the recorded functional, durability, quorum, reconnection and recovery scenarios, including the targeted fix for automatic synchronisation after follower rejoin. These are **manual integration results**, not a guarantee of correctness under all failures or concurrent schedules. The multi-node work was merged into `main` on 2026-11-03; this report documents the test evidence for that merge and should be committed as a follow-up documentation change.
