package kv.server;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import kv.state.StateManager;

public class KVNodeC {
	private static final String NODE_ID = "Casper-3";
	private static final int CLIENT_PORT = 7003; 
	private static final int MY_CLUSTER_PORT = 8003; 
	private static final int NEXT_CLUSTER_PORT = 8002; 
	
	public static Socket nextNodeSocket = null;
	public static Socket prevNodeSocket = null;
	
	public static void main(String[] args) throws IOException {
		
		ServerSocket kvServerSocket = null;
		boolean listening = true;
		
		StateManager stateManager = new StateManager();
		new WorkerThread(stateManager).start();

		try {
			kvServerSocket = new ServerSocket(CLIENT_PORT);
		} catch (IOException e) {
			System.err.println("Could not start " + NODE_ID + " specified port.");
		      System.exit(-1);
		}
		
		System.out.println(NODE_ID + " started");
		
		new Thread(() -> {
		    try (ServerSocket clusterListener = new ServerSocket(MY_CLUSTER_PORT)) {
		        System.out.println(NODE_ID + " listening for previous node on port " + MY_CLUSTER_PORT);
		        while (true) {
		            prevNodeSocket = clusterListener.accept();
		            System.out.println("Previous node connected to " + NODE_ID);
		            
		            // TODO: Create a thread here later to handle incoming node messages
		        }
		    } catch (IOException e) {
		        System.err.println("Cluster listener error on " + NODE_ID + ": " + e.getMessage());
		    }
		}).start();
		
		new Thread(() -> {
		    System.out.println(NODE_ID + " attempting to connect to next node on port " + NEXT_CLUSTER_PORT);
		    while (nextNodeSocket == null) {
		        try {
		            // Hardcoded to localhost for your local testing
		            nextNodeSocket = new Socket("localhost", NEXT_CLUSTER_PORT); 
		            System.out.println(NODE_ID + " successfully connected to next node!");
		            
		            // TODO: Keep track of this socket so we can send data forward later
		            
		        } catch (IOException e) {
		            // Next node isn't awake yet, wait 2 seconds and try again
		            try { Thread.sleep(2000); } catch (InterruptedException ignored) {}
		        }
		    }
		}).start();
		
		while (listening) {
			new KVThread(kvServerSocket.accept(), stateManager).start();
			System.out.println("New " + NODE_ID + " thread started");
		}
		kvServerSocket.close();
	}

}
