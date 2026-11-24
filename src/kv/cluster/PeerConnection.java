package kv.cluster;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class PeerConnection {
	private final String nodeId;
	private final BlockingQueue<String> outgoing= new LinkedBlockingQueue<>();
	
	public PeerConnection(String id) {
		this.nodeId = id;
	}
	
	public void queueMessage(String msg) {
		outgoing.offer(msg);
	}
	
	public String getNextMessage() throws InterruptedException {
		return outgoing.take();
	}
	
	public String getPeerId() {
		return nodeId;
	}
	
	

}
