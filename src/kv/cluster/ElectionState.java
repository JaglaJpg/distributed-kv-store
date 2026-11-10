package kv.cluster;

public class ElectionState {
	private Long currentTerm;
	private String votedFor;
	
	public ElectionState(String[] elecState) {
		this.currentTerm = Long.parseLong(elecState[0]);
		this.votedFor = elecState[1];
	}
	
	public synchronized Long getCurrentTerm() {
		return currentTerm;
	}
	
	public synchronized String getVotedFor() {
		return votedFor;
	}
	
	public synchronized Long startNewTerm() {
		currentTerm++;
		votedFor = null;
		
		return currentTerm;
	}
	
	public synchronized Long observeHigherTerm(Long term) {
		if (term <= currentTerm) {
		    throw new IllegalArgumentException("Term must be higher than current term");
		}
		
		currentTerm = term;
		votedFor = null;
		
		return currentTerm;
	}
	
	public synchronized String voteFor(String nodeId) {
		votedFor = nodeId;
		
		return votedFor;
	}
	
	public synchronized String[] getElectionState() {
		return new String[] {currentTerm.toString(), votedFor};
	}
	

}
