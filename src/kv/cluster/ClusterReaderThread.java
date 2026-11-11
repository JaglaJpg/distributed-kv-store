package kv.cluster;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;

public class ClusterReaderThread extends Thread{
	private PeerConnection peer;
	private Socket socket;
	private BufferedReader in;
	
	private ClusterManager clusterManager;

	public ClusterReaderThread(
	        Socket socket,
	        PeerConnection peer,
	        ClusterManager clusterManager) throws IOException {

	    this.peer = peer;
	    this.socket = socket;
	    this.clusterManager = clusterManager;

	    in = new BufferedReader(
	            new InputStreamReader(socket.getInputStream())
	    );
	}
	
	@Override
	public void run() {
		String line;
		try {
			while ((line = in.readLine()) != null) {
				clusterManager.handleMessage(line, peer);
				
			}
			
		} catch (IOException e) {
			System.out.println("Cluster peer connection lost.");
			
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			
		} finally {
		    try {
		        socket.close();
		    } catch (IOException e) {
		        // Socket already closed.
		    }
		}
		
	}

}
