# Multi-Node Refactor Audit --- Client Thread / Operation Processing Boundary

## Why I stopped here

The first multi-node networking skeleton exposed an architectural
assumption from the single-node version.

I originally described the problem as:

> `StateManager` is an important interface to the data, but it is
> entirely owned by the thread that only handles clients. That makes
> node-to-node work fucking hard to wire in.

That is **conceptually close, but technically needs one correction**
before I refactor anything:

`KVThread` does **not** create or uniquely own a `StateManager`. The
server creates one `StateManager` and passes the same reference into
every `KVThread`.

The real problem is the **worldview of the current execution path**:

``` text
client socket
    ↓
KVThread
    ↓
StateManager.handleCommand(...)
    ↓
validate
lock
WAL append
execute immediately
checkpoint bookkeeping
    ↓
Response
    ↓
client socket
```

At the moment, the only normal doorway into that operation pipeline is a
client thread, and `handleCommand()` assumes the single-node rule:

> if a mutation is accepted, durably append it and then execute it
> locally.

That assumption was correct for the single-node store. It stops being
correct once a mutation can arrive through cluster replication and
**logged / committed / applied become separate stages**.

This is the wall I hit today.

\---

## What I realised while trying to wire in replication

The node now has two completely different sources of work:

``` text
CLIENT WORLD                         CLUSTER WORLD

client request                       message from another node
     ↓                                        ↓
KVClientThread                       ClusterReaderThread
     │                                        │
     └───────────────┐        ┌───────────────┘
                     ↓        ↓
                   local data work
```

The important question was:

> If the leader receives a forwarded request, or a follower receives
> `REPLICATE #81 PUT x y`, which `KVThread` is supposed to do the normal
> database work?

The answer is: **none of them.**

Hijacking an arbitrary client thread would make no sense. Creating a
fake client-style thread for cluster traffic would only preserve the
wrong abstraction.

The client thread should represent **one client connection**, not "the
place where database operations happen."

That means the current `KVThread` should become explicitly a
`KVClientThread` and get even dumber.

\---

## Current `KVThread` audit

`KVThread` currently does three useful client-specific jobs:

1. Owns the client socket and its reader/writer.
2. Reads and tokenises client input.
3. Converts the returned `Response` into human-readable client output.

Those responsibilities belong in the future `KVClientThread`.

It already delegates the actual command to:

``` java
stateManager.handleCommand(tokens);
```

So the thread itself is **less of the problem than I initially
thought**.

The important refactor boundary is underneath it.

### What should remain in `KVClientThread`

``` text
read client line
↓
tokenise / parse client request
↓
delegate request
↓
receive Response
↓
format client-facing text
↓
write to client socket
```

It should not know how replication works, how majority acknowledgement
works, how WAL commitment works, or how another node applies an
operation.

\---

## Current `StateManager` audit

`StateManager` is already doing much of what I would call an
"OperationProcessor."

Its current `handleCommand()` orchestrates:

``` text
validate command
↓
special RESET path if required
↓
enter checkpoint-safe operation
↓
acquire key lock
↓
append mutation to WAL
↓
execute mutation against KVSharedState
↓
increment checkpoint mutation count
↓
release key lock
↓
leave operation
↓
return Response
```

This is important because I should **not create an `OperationProcessor`
and blindly copy all of `StateManager` into it**.

The actual problem is that this orchestration is currently **one
indivisible single-node operation**.

For multi-node writes I need to be able to separate at least these
concepts:

``` text
LOGGED
    operation durably exists in WAL

COMMITTED
    distributed agreement says the ordered prefix is safe

APPLIED
    operation has actually changed the live KV state
```

The existing method effectively performs:

``` text
LOG → APPLY
```

in one call.

The distributed version needs to support:

``` text
LOG
↓
wait for distributed agreement
↓
COMMIT
↓
APPLY
```

That is the real reason a new operation-level orchestration boundary is
needed.

\---

## The three replication indexes

Today I settled the meaning of the three indexes:

``` text
logEnd      = last operation this node HAS in its WAL
commitIndex = last operation this node KNOWS IS SAFE / committed
lastApplied = last operation this node has actually EXECUTED
```

Invariant:

``` text
lastApplied <= commitIndex <= logEnd
```

Important correction to my own wording:

`commitIndex` does **not** mean "safe but not executed."

It means:

> everything through here is safe.

Some of that prefix may already be executed. `lastApplied` tells me how
far execution has actually reached.

Example:

``` text
logEnd      = 85
commitIndex = 83
lastApplied = 80
```

means:

``` text
1–80   logged + committed + applied
81–83  logged + committed, waiting to be applied
84–85  logged only, NOT safe to apply
```

These indexes are replication state and will eventually need proper
persistence/recovery. That is a separate task.

\---

## Current understanding of the healthy write trace

### Request received directly by leader

``` text
client
↓
KVClientThread
↓
operation/cluster orchestration
↓
leader assigns ordered log index
↓
leader durably appends operation
↓
leader logEnd advances
↓
REPLICATE sent to followers
↓
follower durably appends
↓
follower logEnd advances
↓
ACK\_LOGGED returned
↓
leader has majority
↓
leader commitIndex advances
↓
leader applies committed operation(s)
↓
leader lastApplied advances
↓
leader announces COMMIT <commitIndex>
↓
followers apply what they safely possess
```

A follower receiving a leader commit index applies:

``` text
lastApplied + 1 ... commitIndex
```

but only for entries it actually has.

If:

``` text
follower logEnd < leader commitIndex
```

the follower is missing committed operations and needs catch-up.

For the healthy/no-divergence version, the rough catch-up trace is:

``` text
follower sends its logEnd
↓
leader sends missing committed entries
    follower logEnd + 1 ... leader commitIndex
↓
follower durably logs them
↓
follower advances local logEnd
↓
follower can now apply the committed prefix
↓
lastApplied advances
```

Election-era log divergence is deliberately **not part of this stage
yet**.

\---

## Forwarded client requests

There is another path that must eventually use the same machinery:

``` text
client
↓
FOLLOWER KVClientThread
↓
forward request to leader
↓
leader runs normal ordered replication pipeline
↓
operation commits
↓
leader tells originating follower that request X succeeded
↓
originating follower responds to its waiting client
```

This will require request identity/correlation later so the follower
knows **which client request** the leader's result belongs to.

This is another reason cluster work cannot be wired into some arbitrary
`KVThread`.

\---

## Proposed responsibility split

The architecture I am aiming toward is:

``` text
KVClientThread
    = client transport and client-facing response formatting

ClusterReaderThread
    = node-to-node inbound transport

ClusterWriterThread
    = node-to-node outbound transport
      (take queued message and send it)

PeerConnection
    = per-peer outgoing queue / connection-facing shared object

ClusterManager
    = distributed protocol brain
      role, peers, replication messages, ACK/commit decisions,
      forwarding, catch-up, later elections

OperationProcessor
    = shared orchestration boundary for local data operations
      usable regardless of whether work originated from a client
      or from cluster protocol

StateManager / KVSharedState / StorageManager / CheckpointCoordinator
    = retain their narrower state, storage, locking and durability jobs
      rather than pushing everything into ClusterManager
```

The exact `OperationProcessor` API is **not final yet**.

The point of introducing it is not to create another god class. It is to
expose the reusable operation phases that the current single-node
`StateManager.handleCommand()` bundles together.

\---

## Refactor rule

The goal is **not**:

> move everything from `StateManager` into `OperationProcessor`.

The goal is:

> make the local operation machinery callable from more than a
> client-only execution path, and separate the phases that distributed
> agreement now forces apart.

The dependencies should point roughly like this:

``` text
KVClientThread ───────────────┐
                              ↓
                       OperationProcessor
                              ↓
                 local state / storage machinery
                              ↑
                              │
ClusterReader → ClusterManager┘
```

`ClusterManager` should not become the owner of all data logic.

`OperationProcessor` should not become the owner of all cluster logic.

Transport threads should stay dumb.

\---

## Next refactor questions

Before implementing replication, map the smallest useful
`OperationProcessor` API from the **existing code**, rather than
inventing methods from theory.

Questions to answer while doing that:

* What is a genuinely reusable "process a client command" operation?
* What method performs a durable WAL append **without immediately
applying**?
* What method applies an already-logged committed operation **without
appending it again**?
* Where should validation happen for client input versus
trusted/internal replicated operations?
* Where should locking wrap these separated phases?
* How does checkpoint bookkeeping change when "logged" and "applied"
no longer happen together?
* Which object should own `logEnd`, `commitIndex`, and `lastApplied`?
* Which of those indexes can be derived from existing persistence
state, and which must be explicitly persisted?
* How should RESET fit into distributed ordering, if RESET remains
part of the replicated command set?

Do not answer all of these by redesigning the system at once. Use them
to extract the first clean boundary.

\---

## Immediate next session

Start from the existing single-node path and refactor without changing
behaviour first:

``` text
1. Rename KVThread → KVClientThread.
2. Trace StateManager.handleCommand() line by line.
3. Mark each step as:
      client-specific
      operation orchestration
      local state
      storage
      checkpoint/concurrency
4. Sketch the OperationProcessor API from those real responsibilities.
5. Extract the smallest boundary.
6. Run the existing single-node behaviour and durability tests.
7. Only when that remains solid, reconnect ClusterManager to the new operation path.
8. Resume the replication protocol.
```

The objective is that both of these eventually become legitimate entry
points:

``` text
KVClientThread → OperationProcessor

ClusterManager → OperationProcessor
```

without either path pretending to be the other.

\---

## Status at end of session

The multi-node work is **not blocked by an unknown distributed-systems
concept**.

The current blocker is now concrete:

> the single-node command pipeline bundles durable logging and immediate
> application into one client-oriented path, while replication requires
> those stages to be independently callable.

That is the refactor to do next.

The networking skeleton can stay where it is while this boundary is
cleaned up.

