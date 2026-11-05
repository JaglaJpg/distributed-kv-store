package kv.state;

import java.io.IOException;
import java.util.Map;

import kv.cluster.ReplicationState;
import kv.protocol.ExecutionStatus;
import kv.protocol.Response;
import kv.storage.CheckpointCoordinator;
import kv.storage.StorageManager;

public class StateManager {
	private final StorageManager storage;
	private final CheckpointCoordinator checkpoint;
	private final KVSharedState state;
	private final ReplicationState repState;

	public StateManager(StorageManager storage, ReplicationState repState) throws IOException {
	    this.storage = storage;
	    this.state = new KVSharedState();
	    this.repState = repState;
	    
	    this.checkpoint =
	    	    new CheckpointCoordinator(
	    	        state.initialiseState(storage.loadSnapshot(),storage.loadLog(), repState.getLastApplied())
	    	    );
		
	}

	public Response applyOperation(String[] tokens) throws InterruptedException, IOException {
		String command = tokens[0];
		
		if (command.equalsIgnoreCase("RESET")) {
			return handleReset(tokens);
		}
		
		Response response = null;
		String key = tokens[1];
		
		checkpoint.enterOperation();
		
		try {
			if (!state.acquireLock(key, command)) {
				return new Response(ExecutionStatus.ERR_NOT_FOUND, "There is no such key " + key);
			}
			
			try {
				response = state.executeCommand(tokens);
				checkpoint.incrementMutationCount();
				
			} finally {
				state.releaseLock(key, command);
			}
			
		} finally {
			checkpoint.leaveOperation();
		}
		
		return response;
	}
	
	public Response read(String[] tokens) throws InterruptedException, IOException {
		String command = tokens[0];
		Response response = null;
		String key = tokens[1];
		
		checkpoint.enterOperation();
		
		try {
			if (!state.acquireLock(key, command)) {
				return new Response(ExecutionStatus.ERR_NOT_FOUND, "There is no such key " + key);
			}
			
			try {
				response = state.executeCommand(tokens);
				
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
		
		checkpoint.enterExclusiveOperation();
		
		try {
			Map<String, String> storeCopy = state.copyStore();
			storage.writeSnapshot(storeCopy, repState.getLastApplied());
			checkpoint.finishCheckpoint(true);
			
		} catch (IOException e) {
			e.printStackTrace();
			checkpoint.finishCheckpoint(false);
		    
		} finally {
			checkpoint.leaveExclusiveOperation();
		}
	}
	
	public void installSnapshot(Map<String, String> snapshotMap) {
		checkpoint.enterExclusiveOperation();
		
		try {
			state.replaceState(snapshotMap);
			
		} finally {
			checkpoint.leaveExclusiveOperation();
		}
	}
	
	public Response handleReset(String[] tokens) throws IOException {
		Response response;
		checkpoint.enterExclusiveOperation();
		
		try {
			checkpoint.resetMutationCount();
			response = state.executeCommand(tokens);
			
		} finally {
			checkpoint.leaveExclusiveOperation();
		}
		
		return response;
	}
}
