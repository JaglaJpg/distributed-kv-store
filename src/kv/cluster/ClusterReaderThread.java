package kv.cluster;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;

public class ClusterReaderThread extends Thread{
	private PeerConnection peer;
	private Socket socket;
	private BufferedReader in;
	
	public ClusterReaderThread(Socket socket, PeerConnection peer) throws IOException {
		this.peer = peer;
		this.socket = socket;
		in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
	}
	
	public void run() {
		
	}

}
