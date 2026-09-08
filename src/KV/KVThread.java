package KV;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class KVThread extends Thread{
	private Socket KVSocket = null;
	private KVSharedState store;

	public KVThread(Socket KVSocket, KVSharedState store) {
		this.KVSocket = KVSocket;
		this.store = store;
	}

	public void run() {
		System.out.println("Initialising");
		try {
			PrintWriter out = new PrintWriter(KVSocket.getOutputStream(), true);
			BufferedReader in = new BufferedReader(new InputStreamReader(KVSocket.getInputStream()));
			String inputLine, outputLine;

			while((inputLine = in.readLine()) != null) {
				try {
					String[] tokens = inputLine.trim().split("\\s+");
					if(store.validateInput(tokens)) {
						String key = tokens[1];
						String type;

						if(tokens[0].equalsIgnoreCase("put") || tokens[0].equalsIgnoreCase("delete")) {
							type = "writer";
						} else {
							type = "reader";
						}

						if(!store.aquireLock(key, type, tokens[0])) {
							out.println("There is no such key " + key);
							continue;
						}
						

						try {
							outputLine = store.processCommand(tokens);

							String action = tokens[0].toLowerCase();

							if (action.equals("put")) {
							    String value = tokens[2];
							    if (outputLine.equals("OK_ADDED")) { 
							        out.println("Successfully added key '" + key + "' with value '" + value + "'");
							    } else if (outputLine.equals("OK_UPDATED")) { 
							        out.println("Successfully updated key '" + key + "' to new value '" + value + "'");
							    }
							} 
							else if (action.equals("delete")) {
							    if (outputLine.equals("OK_DELETED")) { 
							        out.println("Successfully deleted key '" + key + "'");
							    } else if (outputLine.equals("ERR_NOT_FOUND")) { 
							        out.println("Error: Could not delete because key '" + key + "' does not exist");
							    }
							} 
							else if (action.equals("get")) {
							    if (outputLine.equals("ERR_NOT_FOUND")) { 
							        out.println("Error: Key '" + key + "' does not exist");
							    } else {
							        out.println("Value for '" + key + "': " + outputLine);
							    }
							}
						} finally {
							store.releaseLock(key, type);
						}
					} else {
						outputLine = "Received incorrect request - only understand: "
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
			KVSocket.close();

		} catch (IOException e) {

			e.printStackTrace();
		}

	}

}
