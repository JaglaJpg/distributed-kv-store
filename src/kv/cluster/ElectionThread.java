package kv.cluster;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class ElectionThread extends Thread{
	private ClusterManager clusterManager;
	private Long lastHeartBeatTime = 0L;
	
	public ElectionThread(ClusterManager clusterManager) {
		this.clusterManager = clusterManager;
		
	}
	
	@Override
	public void run() {
		while(true) {
			NodeRole role = clusterManager.getNodeRole();
			
			switch(role) {
			
			case LEADER:
				Long currentTime = System.nanoTime();
				
				if (lastHeartBeatTime == 0 || currentTime -lastHeartBeatTime >= TimeUnit.MILLISECONDS.toNanos(200)) {
					clusterManager.sendHeartbeat();
					lastHeartBeatTime = currentTime;
				}
				
				break;
				
			case CANDIDATE:
				if (clusterManager.hasTimedOut()) {
					try {
						clusterManager.startElection();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				
				break;
				
			case FOLLOWER:
				break;
				
			default:
				break;
			}
			
			
			try {
				Thread.sleep(20);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				return;
			}
		}
	}

}
