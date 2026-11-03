package kv.server;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.concurrent.ExecutionException;

import kv.protocol.ExecutionStatus;
import kv.protocol.Response;
import kv.cluster.ClusterManager;
import kv.cluster.OperationProcessor;
import kv.state.StateManager;

public class KVClientThread extends Thread {

    private final Socket kvSocket;
    private final StateManager stateManager;
    private final OperationProcessor operationProcessor;
    private final ClusterManager cluster;

    public KVClientThread(
            Socket kvSocket,
            StateManager stateManager,
            OperationProcessor operationProcessor,
            ClusterManager clusterManager) {

        this.kvSocket = kvSocket;
        this.stateManager = stateManager;
        this.operationProcessor = operationProcessor;
        this.cluster = clusterManager;
    }

    public void run() {
        System.out.println("Initialising");

        try (
        	    PrintWriter out =
        	        new PrintWriter(kvSocket.getOutputStream(), true);

        	    BufferedReader in =
        	        new BufferedReader(
        	            new InputStreamReader(kvSocket.getInputStream())
        	        )
        	) {

            String inputLine;

            while ((inputLine = in.readLine()) != null) {
                try {
                    String[] tokens = inputLine.trim().split("\\s+");

                    if (!operationProcessor.validateInput(tokens)) {
                        out.println(
                            "Received incorrect request - only understand: "
                            + "\"PUT <key> <value>\", "
                            + "\"GET <key>\", "
                            + "\"DELETE <key>\""
                        );
                        continue;
                    }

                    String action = tokens[0];
                    Response response;

                    if (action.equalsIgnoreCase("GET")) {

                        if(cluster.isReady()) {
                        	response = stateManager.read(tokens);
                        } else {
                        	response = new Response(ExecutionStatus.NOT_READY, "This Node is synchronising, retry shortyly");  
                        }
                        

                    } else {
                        response = cluster.handleClientMutation(tokens);
                    }

                    ExecutionStatus status = response.getStatus();
                    
                    if (status == ExecutionStatus.ERR_TIMEOUT) {
                        out.println("Error: Write timed out. Commit status unknown.");
                        continue;
                    }

                    if (action.equalsIgnoreCase("reset")) {
                        out.println(response.getPayload());

                    } else if (action.equalsIgnoreCase("put")) {
                        String key = tokens[1];
                        String value = tokens[2];

                        if (status == ExecutionStatus.OK_ADDED) {
                            out.println(
                                "Successfully added key '" + key
                                + "' with value '" + value + "'"
                            );
                        } else if (status == ExecutionStatus.OK_UPDATED) {
                            out.println(
                                "Successfully updated key '" + key
                                + "' to new value '" + value + "'"
                            );
                        }

                    } else if (action.equalsIgnoreCase("delete")) {
                        String key = tokens[1];

                        if (status == ExecutionStatus.OK_DELETED) {
                            out.println(
                                "Successfully deleted key '" + key + "'"
                            );
                        } else if (status == ExecutionStatus.ERR_NOT_FOUND) {
                            out.println(
                                "Error: Could not delete because key '"
                                + key + "' does not exist"
                            );
                        }

                    } else if (action.equalsIgnoreCase("get")) {
                        String key = tokens[1];

                        if (status != ExecutionStatus.ERR_NOT_FOUND) {
                            out.println(
                                "Value for '" + key + "': "
                                + response.getPayload()
                            );
                        } else {
                            out.println(
                                "Error: Key '" + key + "' does not exist"
                            );
                        }
                    }

                } catch (InterruptedException e) {
                    System.err.println(
                        "Failed to get lock when reading: " + e
                    );
                } catch (ExecutionException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
            }
            
            kvSocket.close();

        } catch (IOException e) {
        	System.out.println("Client disconnected.");
        }
    }
}