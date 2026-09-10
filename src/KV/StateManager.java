package kv;

import java.io.IOException;
import java.util.Map;

public class StateManager {
	private final StorageManager storage;
	private final CheckpointCoordinator checkpoint;
	private final KVSharedState state;

	public StateManager() throws IOException {
	    this.storage = new StorageManager();
	    this.state = new KVSharedState();
	    
	    this.checkpoint =
	    	    new CheckpointCoordinator(
	    	        state.initialiseState(storage.loadSnapshot(),storage.loadLog())
	    	    );
		
	}


	public Response handleCommand(String[] tokens) throws InterruptedException, IOException {
		if(!state.validateInput(tokens)) {
			return new Response(ExecutionStatus.INVALID_COMMAND);
		}
		
		Response response = null;
		String command = tokens[0];
		String key = tokens[1];
		
		boolean isMutation = !command.equalsIgnoreCase("GET");
		
		checkpoint.enterOperation();
		
		try {
			if (!state.acquireLock(key, command)) {
				return new Response(ExecutionStatus.ERR_NOT_FOUND, "There is no such key " + key);
			}
			
			try {
				if(isMutation) {
					storage.appendOperation(tokens);
				}
				
				response = state.executeCommand(tokens);
				
				if(isMutation) {
					checkpoint.incrementMutationCount();
				}				
				
				
			} finally {
				state.releaseLock(key, command);
			}
			
		} finally {
			checkpoint.leaveOperation();
		}
		
		return response;
	}
	
	public void waitForCheckpointSignal() {
		checkpoint.waitForSignal();
	}
	
	public void performCheckpoint()  {
		checkpoint.requestCheckpoint();
		
		checkpoint.enterCheckpoint();
		
		try {
			Map<String, String> storeCopy = state.copyStore();
			storage.writeSnapshot(storeCopy);
			checkpoint.finishCheckpoint(true);
			
		} catch (IOException e) {
			e.printStackTrace();
			checkpoint.finishCheckpoint(false);
		    
		} finally {
			checkpoint.leaveCheckpoint();
		}
	}
}
