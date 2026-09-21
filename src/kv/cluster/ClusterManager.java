package kv.cluster;

public class ClusterManager {
	
	private final String nodeId;
    private NodeRole role;

    private long logEnd;
    private long commitIndex;
    private long lastApplied;
	
	public ClusterManager(String NODE_ID, NodeRole role) {
		this.nodeId = NODE_ID;
		this.role = role;
	}
	
 	public void handleMessage(String msg, PeerConnection peer) {
 		
        if (role == NodeRole.LEADER) {
            handleAsLeader(msg, peer);
        } else {
            handleAsFollower(msg, peer);
        }
        
    }
 	
 	public void handleAsLeader(String msg, PeerConnection peer) {
 		
 	}
 	
 	public void handleAsFollower(String msg, PeerConnection peer) {
 		
 	}
 	
 	

}
