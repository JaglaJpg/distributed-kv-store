package kv;

import java.util.Map;

public class Snapshot {
	private Map<String, String> snapshot;
	private Long checkNum;
	
	public Snapshot(Map<String, String> snapshot, Long checkNum) {
		this.snapshot = snapshot;
		this.checkNum = checkNum;
	}
	
	public Map<String, String> getSnapshot() {
		return this.snapshot;
	}
	
	public Long getCheckNum() {
		return this.checkNum;
	}

}
