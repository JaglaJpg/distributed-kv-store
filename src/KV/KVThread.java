package kv;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class KVThread extends Thread {
	private Socket kvSocket = null;
	private KVSharedState mySharedState;

	public KVThread(Socket kvSocket, KVSharedState mySharedState) {
		this.kvSocket = kvSocket;
		this.mySharedState = mySharedState;
	}

	public void run() {
		System.out.println("Initialising");
		try {
			PrintWriter out = new PrintWriter(kvSocket.getOutputStream(), true);
			BufferedReader in = new BufferedReader(new InputStreamReader(kvSocket.getInputStream()));
			String inputLine;

			while ((inputLine = in.readLine()) != null) {
				try {
					String[] tokens = inputLine.trim().split("\\s+");
					if (mySharedState.validateInput(tokens)) {
						String key = tokens[1];

						mySharedState.enterGlobalState();
						
						try {
							if (!mySharedState.acquireLock(key, tokens[0])) {
								out.println("There is no such key " + key);
								continue;
							}
							
							try {
								Response response = mySharedState.processCommand(tokens);
								ExecutionStatus status = response.getStatus();

								String action = tokens[0].toLowerCase();

								if (action.equals("put")) {
								    String value = tokens[2];
								    if (status == ExecutionStatus.OK_ADDED) { 
								        out.println("Successfully added key '" + key + "' with value '" + value + "'");
								    } else if (status == ExecutionStatus.OK_UPDATED) { 
								        out.println("Successfully updated key '" + key + "' to new value '" + value + "'");
								    }
								} 
								else if (action.equals("delete")) {
								    if (status == ExecutionStatus.OK_DELETED) { 
								        out.println("Successfully deleted key '" + key + "'");
								    } else if (status == ExecutionStatus.ERR_NOT_FOUND) { 
								        out.println("Error: Could not delete because key '" + key + "' does not exist");
								    }
								} 
								else if (action.equals("get")) {
								    if (status != ExecutionStatus.ERR_NOT_FOUND) { 
								    	out.println("Value for '" + key + "': " + response.getPayload());
								    } else {
								        out.println("Error: Key '" + key + "' does not exist");
								    }
								}
							} finally {
								mySharedState.releaseLock(key, tokens[0]);
							}
						} finally {
							mySharedState.leaveGlobalState();
						}
						
						
					} else {
						String outputLine = "Received incorrect request - only understand: "
								+ "\"PUT <key> <value>\", "
								+ "\"GET <key>\", "
								+ "\"DELETE <key>\"";

						out.println(outputLine);
					}



				} catch(InterruptedException e) {
					System.err.println("Failed to get lock when reading:"+e);
				}
			}
			
			out.close();
			in.close();
			kvSocket.close();

		} catch (IOException e) {

			e.printStackTrace();
		}

	}

}
