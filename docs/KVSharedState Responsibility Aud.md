## KVSharedState Responsibility Audit: 10.09.2026

### Recovery / Bootstrap
- `initialiseState()`

### Per-Key Locking
- `acquireLock()`
- `releaseLock()`

### Command Execution
- `executeCommand()`
- `processCommand()`

### Validation
- `validateInput()`

### Persistence Coordination
- `createSnapshot()`

### Checkpoint Coordination
- `enterGlobalState()`
- `leaveGlobalState()`
- `enterGlobalStateForBackup()`
- `leaveGlobalStateFromBackup()`
- `requestCheckpoint()`
- `completeCheckpoint()`
- `failCheckpoint()`



## Verdict: Too many responsibilities