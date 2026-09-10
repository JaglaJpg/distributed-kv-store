package kv;

import java.io.IOException;
import java.net.ServerSocket;

public class KVServer {
	private static final String DEFAULT_SERVER_NAME = "localhost";
	private static final int DEFAULT_SERVER_PORT = 4545; 
	
	public static void main(String[] args) throws IOException {
		
		ServerSocket kvServerSocket = null;
		boolean listening = true;
		
		StateManager stateManager = new StateManager();
		new WorkerThread(stateManager).start();

		try {
			kvServerSocket = new ServerSocket(DEFAULT_SERVER_PORT);
		} catch (IOException e) {
			System.err.println("Could not start " + DEFAULT_SERVER_NAME + " specified port.");
		      System.exit(-1);
		}
		
		System.out.println(DEFAULT_SERVER_NAME + " started");
		
		while (listening) {
			new KVThread(kvServerSocket.accept(), stateManager).start();
			System.out.println("New " + DEFAULT_SERVER_NAME + " thread started");
		}
		kvServerSocket.close();
	}

}
