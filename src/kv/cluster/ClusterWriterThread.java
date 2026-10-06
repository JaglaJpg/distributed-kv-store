package kv.cluster;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;

public class ClusterWriterThread extends Thread{
	private PeerConnection peer;
	private Socket socket;
	private PrintWriter out;
	
	public ClusterWriterThread(Socket socket, PeerConnection peer) throws IOException {
		this.peer = peer;
		this.socket = socket;
		out = new PrintWriter(socket.getOutputStream(), true);
	}

	public void run() {
		while(true) {
			try {
				String message = peer.getNextMessage();
				out.println(message);
				
				if (out.checkError()) {
	                return;
	            }
				
			} catch (InterruptedException e) {
			    Thread.currentThread().interrupt();
			    return;
			}
		}
		
	}
	
}
