package kv.cluster;

public class ReplicationState {

    private Long commitIndex;
    private Long logEnd;
    private Long lastApplied;

    public ReplicationState(Long[] repState) {
        this.lastApplied = repState[0];
        this.commitIndex = repState[1];
        this.logEnd = repState[2];
    }

    public synchronized Long getLogEnd() {
        return logEnd;
    }

    public synchronized Long getCommitIndex() {
        return commitIndex;
    }

    public synchronized Long getLastApplied() {
        return lastApplied;
    }

    public synchronized long incrementLogEnd() {
        logEnd++;
        return logEnd;
    }

    public synchronized long advanceCommitIndex(Long newCommitIndex) {
        if (newCommitIndex < commitIndex) {
            throw new IllegalArgumentException("commitIndex cannot move backwards");
        }

        if (newCommitIndex > logEnd) {
            throw new IllegalArgumentException("commitIndex cannot exceed logEnd");
        }

        commitIndex = newCommitIndex;
        return commitIndex;
    }

    public synchronized long incrementLastApplied() {
        if (lastApplied + 1 > commitIndex) {
            throw new IllegalStateException("lastApplied cannot exceed commitIndex");
        }

        lastApplied++;
        return lastApplied;
    }

    public synchronized void installSnapshotBaseline(Long boundary) {
    	commitIndex = boundary;
    	logEnd = boundary;
    	lastApplied = boundary;
    }
    
    public synchronized void reconcileSnapshotBoundary(long boundary) {
        logEnd = Math.max(logEnd, boundary);
        commitIndex = Math.max(commitIndex, boundary);
        lastApplied = Math.max(lastApplied, boundary);
    }
    
    public Long[] getRepState() {
        return new Long[] {lastApplied, commitIndex, logEnd};
    }
}