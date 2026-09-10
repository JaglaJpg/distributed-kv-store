# Architecture

## Startup and Recovery Flow

1. KVServer creates BackupSignal.
2. KVServer creates StorageManager.
3. KVServer creates KVSharedState.
4. KVServer spawns WorkerThread.
5. KVSharedState loads the latest snapshot.
6. KVSharedState loads the remaining operation log.
7. KVSharedState replays log operations into the live store.
8. KVServer starts WorkerThread.
9. KVServer opens the server socket.
10. KVServer begins accepting clients.


## GET Flow

1. Client sends GET.
2. KVThread parses command.
3. KVThread acquires global admission read lock for the shared state.
4. KVThread acquires the read lock for the data.
5. KVSharedState processes the command and returns the requested data.
6. KVThread sends response back to client.
7. Locks are released.


## PUT Flow

1. Client sends PUT.
2. KVThread parses command.
3. KVThread acquires global admission read lock for the shared state.
4a. KVThread acquires the write lock for the data.
4b. If data doesn't exist yet, KVSharedState creates a lock for the data and lets KVThread acquire it.
5. KVSharedState calls on StorageManager to append the operation to the log.
6. Increment the mutation count.
7a. If the count is more than or equal to 1000, request checkpoint.
7b. If the count is less than 1000, do nothing.
8. Apply mutation to the data.
9. KVThread responds to Client.
10. Locks released.


## DELETE Flow

1. Client sends DELETE.
2. KVThread parses command.
3. KVThread acquires global admission read lock for the shared state.
4a. KVThread acquires the write lock for the data.
4b. If data doesn't exist, respond with ERR_NOT_FOUND.
5. KVSharedState calls on StorageManager to append the operation to the log.
6. Increment the mutation count.
7a. If the count is more than or equal to 1000, request checkpoint.
7b. If the count is less than 1000, do nothing.
8. Apply mutation to the data.
9. KVThread responds to Client.
10. Locks released.


## Checkpoint Flow

1. Mutation count hits 1000 operation threshold.
2. KVSharedState requests a checkpoint after detecting the threshold.
3. Subsequent client threads are blocked from entering global state.
4. KVSharedState triggers signal for a backup.
5. WorkerThread wakes.
6. WorkerThread aquires global admission write lock.
7. New snapshot is written.
8. New snapshot replaces old snapshot.
9. Log truncated
10. Signal that checkpoint is completed.
11. Waiting Clients are released.
12. Worker releases write lock.


## Startup and Recovery Flow

1. KVServer creates BackupSignal.
2. KVServer creates StorageManager.
3. KVServer creates KVSharedState.
4. KVSharedState loads the latest snapshot.
5. KVSharedState loads the remaining operation log.
6. KVSharedState replays log operations into the live store.
7. KVServer creates and starts WorkerThread.
8. KVServer opens the server socket.
9. KVServer begins accepting clients.
