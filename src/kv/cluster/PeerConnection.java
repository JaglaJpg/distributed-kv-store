package kv.cluster;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class PeerConnection {
	private final BlockingQueue<String> outgoing= new LinkedBlockingQueue<>();
	
	public void queueMessage(String msg) {
		outgoing.offer(msg);
	}
	
	public String getNextMessage() throws InterruptedException {
		return outgoing.take();
	}
	
	

}
