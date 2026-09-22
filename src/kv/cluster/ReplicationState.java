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

    public Long getLogEnd() {
        return logEnd;
    }

    public Long getCommitIndex() {
        return commitIndex;
    }

    public Long getLastApplied() {
        return lastApplied;
    }

    public void incrementLogEnd() {
        logEnd++;
    }

    public void advanceCommitIndex(Long newCommitIndex) {
        if (newCommitIndex < commitIndex) {
            throw new IllegalArgumentException("commitIndex cannot move backwards");
        }

        if (newCommitIndex > logEnd) {
            throw new IllegalArgumentException("commitIndex cannot exceed logEnd");
        }

        commitIndex = newCommitIndex;
    }

    public void incrementLastApplied() {
        if (lastApplied + 1 > commitIndex) {
            throw new IllegalStateException("lastApplied cannot exceed commitIndex");
        }

        lastApplied++;
    }

    public Long[] getRepState() {
        return new Long[] {lastApplied, commitIndex, logEnd};
    }
}