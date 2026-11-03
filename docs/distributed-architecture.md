DISTRIBUTED KV STORE — ARCHITECTURE NOTES
==========================================

GOAL
----
Extend the existing durable single-node KV store into 3 identical,
replicated data nodes.

Main goals:
- Clients can connect to any healthy node.
- Data is replicated so one machine failing doesn't destroy the database.
- All replicas eventually contain one authoritative mutation history.
- Keep the project bounded to 3 nodes rather than designing for massive scale.


WHY NOT LET EVERY NODE WRITE INDEPENDENTLY?
--------------------------------------------
Original possible design:

    Client 1 -> A: SET x CAT
    Client 2 -> B: SET x DOG

If A and B independently execute these writes, different histories can
develop simultaneously.

This creates two problems:

1. Determining the relationship/order between independently created writes.
2. Resolving conflicts when those writes are concurrent.

Vector clocks can help with (1).

A vector clock describes CAUSAL ordering, not real-world physical time.

It can determine:
- X happened-before Y
- Y happened-before X
- X and Y are concurrent

However, if two conflicting writes are concurrent, the vector clock DOES NOT
tell us which value should win.

That requires an additional conflict-resolution system.

Therefore, instead of solving independently diverging histories, this project
will PREVENT them by using one current leader.


CHOSEN ARCHITECTURE — SINGLE CURRENT LEADER
---------------------------------------------

At a particular moment:

             A
          [LEADER]
          /      \
         B        C
    [FOLLOWER] [FOLLOWER]

Clients may still connect to A, B, or C.

However:

- Only the current leader establishes authoritative write order.
- Followers do NOT independently execute client writes.
- A follower forwards client writes to the current leader.

Example:

    Client -> B: SET x CAT

    B -> A: SET x CAT

    A assigns:

        #81 SET x CAT

If another request arrives:

        #82 SET x DOG

The leader has therefore converted concurrent incoming requests into one
authoritative sequential history.

Requests can still ARRIVE and be processed concurrently.

Only the point at which mutations enter the authoritative database history
is serialized.


VECTOR CLOCKS
-------------
Vector clocks were useful for understanding a leaderless design.

They are no longer required for the core replication mechanism because
followers do not independently create authoritative histories.

Instead, the leader's ordered log establishes mutation ordering:

    #81
    #82
    #83
    ...

The log index therefore replaces the ordering problem that originally led
us toward vector clocks.


LEADERSHIP MODEL
----------------
Leadership is a ROLE, not a different type of server.

Bad conceptual model:

    LeaderServer
    FollowerServer

Better:

    KVNode
        role = LEADER | FOLLOWER

All nodes contain the capabilities required to perform either role.

Initially:

    A = LEADER
    B = FOLLOWER
    C = FOLLOWER

A remains leader indefinitely while healthy.

If A fails, the remaining cluster will eventually elect another leader.

Example:

    A fails

    B = LEADER
    C = FOLLOWER

If A later returns:

    A = FOLLOWER
    B = LEADER
    C = FOLLOWER

A does not automatically reclaim leadership merely because it was the
original leader.

Leader election mechanics have NOT been designed yet.


NETWORK TOPOLOGY
----------------
For 3 nodes, use FULL MESH connectivity:

        A
       / \
      B---C

Every node knows how to communicate directly with every other node.

During normal operation, replication traffic will mostly look like a star:

        B
        ^
        |
        A ----> C
      LEADER

However, the cluster still has full-mesh connectivity because:
- Any node may later become leader.
- Future leader elections require node-to-node communication.
- Recovery/catch-up may require peer communication.

Full mesh is acceptable because this project is deliberately limited to
3 nodes.

It would scale poorly to hundreds/thousands of nodes because every node
would need relationships with many peers and network traffic could become
huge.


TWO COMMUNICATION PATHS
-----------------------
Keep user communication and distributed-system communication logically
separate.

1. CLIENT <-> NODE

Used for user database operations:

    GET
    SET
    DELETE
    INCREMENT
    etc.

2. NODE <-> NODE

Used internally by the distributed system:

    forwarding writes
    replicating log entries
    acknowledgements
    communicating commit progress
    recovery/catch-up
    eventually leader election

Potential implementation:

    Node A:
        clientPort
        clusterPort

This keeps the external database protocol separate from the internal
cluster protocol.


WRITE PIPELINE
--------------
Assume:

    A = LEADER
    B = FOLLOWER
    C = FOLLOWER

Client connects to B:

    INCREMENT x


STEP 1 — FORWARD
----------------
B receives the client request.

B does NOT modify x.

B does NOT yet create an authoritative WAL entry.

B remembers enough information to associate the eventual result with the
waiting client.

B forwards the request to A.


STEP 2 — ORDER
--------------
A receives the forwarded write.

A assigns the next authoritative log position:

    #81 INCREMENT x

The log index defines where this mutation exists in database history.


STEP 3 — LEADER DURABLY LOGS
-----------------------------
A appends:

    81 | INCREMENT x

to its durable log/WAL.

Important:

    LOGGED != COMMITTED != APPLIED

The existence of #81 in the WAL does not automatically mean x should
already change.


STEP 4 — REPLICATE
------------------
A sends the ordered entry to B and C:

             #81
        +------------> B
        |
        A
        |
        +------------> C
             #81

Replication to B and C should be able to happen concurrently.

Do NOT design:

    send to B
    wait completely
    send to C

because a slow/dead B would unnecessarily prevent C from responding.


STEP 5 — FOLLOWERS DURABLY LOG
-------------------------------
When B/C receive #81, they append it to their own durable logs.

Example:

    B WAL:
        81 | INCREMENT x

After the entry is durably stored, the follower acknowledges it:

    B -> A: ACK #81

Important:

The follower having #81 in its WAL does NOT necessarily mean it should
apply #81 to its live KV state yet.


STEP 6 — MAJORITY / QUORUM
---------------------------
There are 3 nodes.

A majority therefore requires 2 nodes.

If:

    A has #81
    B has #81
    C is slow/unavailable

then #81 exists durably on 2/3 nodes.

The leader can therefore consider #81 COMMITTED.


WHY MAJORITY MATTERS
--------------------
Possible 2-node majorities are:

    A + B
    A + C
    B + C

Any majority overlaps every other possible majority.

Example:

#81 commits on:

    A + B

Then A dies.

The surviving majority is:

    B + C

B participated in both groups and therefore knows about #81.

This quorum-overlap property becomes important when leadership changes.

The full leader-election consequences will be learned later.


STEP 7 — COMMIT
---------------
Once the leader knows a majority durably possesses #81:

    commitIndex = 81

This means:

    "Every log entry through #81 is known to be committed."


STEP 8 — APPLY
--------------
Committed entries can now be executed against the actual KV state.

Before:

    x = 10

Apply:

    #81 INCREMENT x

After:

    x = 11

The node updates:

    lastApplied = 81


STEP 9 — FOLLOWERS LEARN COMMIT PROGRESS
-----------------------------------------
Followers may already HAVE #81 in their WAL without knowing it has committed.

The leader therefore communicates its commit progress:

    leaderCommit = 81

B/C can then apply their logged entries through #81.

This allows all replicas to move their actual KV state through the same
authoritative history.


STEP 10 — CLIENT SUCCESS
------------------------
Once the operation has reached the required committed/applied state, the
result travels back:

    A -> B -> Client

    SUCCESS


COMPRESSED WRITE PIPELINE
-------------------------

    CLIENT
       |
       v
    FOLLOWER
       |
       | forward
       v
    LEADER
       |
       | assign log position
       v
    LEADER WAL
       |
       | replicate
       v
    FOLLOWER WALs
       |
       | ACK
       v
    MAJORITY
       |
       v
    COMMIT
       |
       v
    APPLY
       |
       v
    SUCCESS


THREE IMPORTANT LOG POSITIONS
-----------------------------

1. logEnd

Highest log entry currently stored.

2. commitIndex

Highest log entry known to be committed.

Everything <= commitIndex is committed.

3. lastApplied

Highest committed log entry already executed against the actual KV state.


Important invariant:

    lastApplied <= commitIndex <= logEnd


Example:

    logEnd      = 84
    commitIndex = 82
    lastApplied = 81

Meaning:

    #81  committed + applied
    #82  committed, waiting to be applied
    #83  logged, NOT committed
    #84  logged, NOT committed


PIPELINING / CONCURRENCY
------------------------
The leader does NOT have to completely finish one request before beginning
the next.

It may currently have:

    logEnd      = 84
    commitIndex = 82
    lastApplied = 81

while simultaneously receiving and ordering newer requests.

Therefore the system is not:

    receive write
    freeze everything
    replicate
    commit
    apply
    next write

Instead, multiple stages of different requests can be progressing
concurrently.

The authoritative ORDER is serialized.

The entire database implementation does not have to become single-threaded.


FIRST IMPLEMENTATION MILESTONE
------------------------------
DO NOT begin with:
- replication
- quorum
- leader election
- failure detection
- Raft

First build CLUSTER PLUMBING.

Run three identical KV node processes.

Example:

    Node A
        nodeId      = A
        clientPort  = 7001
        clusterPort = 8001

    Node B
        nodeId      = B
        clientPort  = 7002
        clusterPort = 8002

    Node C
        nodeId      = C
        clientPort  = 7003
        clusterPort = 8003

Each node knows:
- its own stable identity
- its current role
- peer node addresses


FIRST TARGET
------------
Make basic internal communication work:

    A -> B:
        HELLO(A)

    B -> A:
        HELLO_ACK(B)

using the NODE <-> NODE protocol.

No database replication is required yet.

Once that works, build incrementally:

    cluster plumbing
        ↓
    node roles
        ↓
    follower write forwarding
        ↓
    ordered replicated log
        ↓
    acknowledgements + quorum
        ↓
    commit/apply pipeline
        ↓
    failure detection
        ↓
    leader election / recovery


NOT YET
-------
Do not prematurely solve:

- Leader election
- Election terms
- Failure detection
- Split-brain
- Divergent log recovery
- Node catch-up
- Snapshot transfer
- Idempotency/retried requests
- Raft details

Those are later problems.

Current goal:

GET THREE IDENTICAL JVM NODES TALKING TO EACH OTHER CLEANLY.