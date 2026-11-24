package kv.cluster;

public class FollowerProgress {
	
	public long nextIndex;
	public long matchIndex;
	
	public FollowerProgress(long nextIndex, long matchIndex) {
		this.nextIndex = nextIndex;
		this.matchIndex = matchIndex;
	}

}
