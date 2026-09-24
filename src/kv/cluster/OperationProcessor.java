package kv.cluster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import kv.protocol.ExecutionStatus;
import kv.protocol.Response;
import kv.state.StateManager;
import kv.storage.StorageManager;

public class OperationProcessor {
	private final ReplicationState repState;
	private final StorageManager storage;
	private final StateManager state;


	public OperationProcessor(ReplicationState repState, StorageManager storage, StateManager state) {
		this.repState = repState;
		this.storage = storage;
		this.state = state;
	}

	public void persistOperation(String[] tokens) throws IOException{
		storage.appendOperation(tokens);
		repState.incrementLogEnd();
		storage.saveRepState(repState.getRepState());

	}

	public Response processLocalMutation(String[] tokens)
			throws IOException, InterruptedException {

		persistOperation(tokens);

		// Temporary single-node behaviour:
		// this node alone is currently enough to consider the entry committed.
		repState.advanceCommitIndex(repState.getLogEnd());
		storage.saveRepState(repState.getRepState());

		return applyCommittedOperation(tokens);
	}

	public List<Response> applyCommittedThrough()
	        throws InterruptedException, IOException {

	    List<Response> responses = new ArrayList<>();
	    List<String[]> log = storage.loadLog();

	    Long lastApplied = repState.getLastApplied();
	    Long commitIndex = repState.getCommitIndex();

	    for (String[] operation : log) {

	        Long index = Long.parseLong(operation[0]);

	        if (index > lastApplied && index <= commitIndex) {

	            responses.add(
	                applyCommittedOperation(
	                    Arrays.copyOfRange(operation, 1, operation.length)
	                )
	            );
	        }
	    }

	    return responses;
	}

	public Response applyCommittedOperation(String[] tokens) 
			throws InterruptedException, IOException {

		Response response = null;

		if(!validateInput(tokens)) {
			return new Response(ExecutionStatus.INVALID_COMMAND);
		}

		response = state.applyOperation(tokens);
		repState.incrementLastApplied();
		storage.saveRepState(repState.getRepState());

		return response;
	}

	public boolean validateInput(String[] input) {
		if (input == null || input.length == 0) {
			return false;
		}

		if (input[0].equalsIgnoreCase("delete") || input[0].equalsIgnoreCase("get")) {
			return input.length == 2;
		} else if (input[0].equalsIgnoreCase("put")) {
			return input.length == 3;
		} else if (input[0].equalsIgnoreCase("reset")) {
			return input.length == 1;
		}

		return false;
	}
	
	public List<String[]> loadCatchUpFromLog(long fromExclusive, long toInclusive)
	        throws IOException {

	    List<String[]> log = storage.loadLog();
	    List<String[]> result = new ArrayList<>();

	    for (String[] entry : log) {
	        long index = Long.parseLong(entry[0]);

	        if (index > fromExclusive && index <= toInclusive) {
	            result.add(entry);
	        }
	    }

	    return result;
	}
}
