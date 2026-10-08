# Cluster Regression Test Report

**Date:** 2026-10-08  
**System:** Three-node distributed key-value store  
**Nodes:** Melchior, Casper, Balthasar  
**Method:** Manual integration testing using the existing local three-node setup  
**Scope:** Basic operations, replication, peer connection lifecycle, follower recovery, snapshots, and persistence

## Summary

| Result | Count |
|---|---:|
| PASS | 10 |
| FAIL | 0 |
| NOT RUN | 0 |

All ten planned regression scenarios passed according to the observed behaviour reported during manual testing. These results demonstrate the tested scenarios, not exhaustive correctness under concurrency, arbitrary crash points, or network partitions.

## Test Results

| ID | Scenario | Observed result | Status |
|---|---|---|---|
| T01 | Cluster startup | Nodes that started earlier retried connections approximately every two seconds; all three connected once running. | PASS |
| T02 | Basic PUT/GET/DELETE | PUT, GET and DELETE worked through clients connected to any node. | PASS |
| T03 | Cross-node consistency | Data written through one node was read through another and deleted through a third. | PASS |
| T04 | Follower disconnect | Predecessor continued retrying; successor detected disconnection and kept listening. | PASS |
| T05 | Follower reconnection | Restarted node connected to its successor and accepted its predecessor. | PASS |
| T06 | Missed-operation catch-up | Follower caught up after the first operation following reconnection. | PASS |
| T07 | Snapshot catch-up | Follower recovering across checkpoint boundaries received the correct snapshot and subsequent log entries. | PASS |
| T08 | Persistence across restart | Data persisted after restart. | PASS |
| T09 | Repeated follower disconnects | Repeated disconnect/restart cycles continued to reconnect successfully. | PASS |
| T10 | Post-recovery replication | New operations replicated after recovery. | PASS |

## Additional Observation

RESET was also exercised and reportedly worked, but was not part of the ten formally enumerated cases. Its precise test steps and expected/actual state transitions were not recorded, so it is not counted separately in the summary.

## Behavioural Notes

- Connection retries were observed at roughly two-second intervals while peers were unavailable.
- Catch-up in T06 was observed after the first post-reconnection operation; immediate idle catch-up was not established by this test.
- The report records user-observed results. Automated assertions, logs, operation counts, and exact crash timings were not captured.

## Not Yet Covered

- Concurrent writes from multiple clients and ordering/consistency assertions.
- Checkpointing under concurrent write load.
- Persistence failure during WAL append or replication-state save.
- Crashes during snapshot installation and associated recovery atomicity.
- Network partitions and leader election (not implemented in this static-leader phase).

## Assessment

**Manual regression suite: PASS (10/10).** The current three-node implementation has passed its defined functional and recovery smoke/integration scenarios. A focused correctness review and targeted concurrency/failure tests remain appropriate before treating the branch as merge-ready.
