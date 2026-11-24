package kv.cluster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import kv.protocol.ExecutionStatus;
import kv.protocol.Response;
import kv.state.StateManager;
import kv.storage.StorageManager;

/**
 * 
 */
public class OperationProcessor {
	private final ReplicationState repState;
	private final StorageManager storage;
	private final StateManager state;


	public OperationProcessor(ReplicationState repState, StorageManager storage, StateManager state) {
		this.repState = repState;
		this.storage = storage;
		this.state = state;
	}

	public synchronized Long persistOperation(long term, String[] tokens) throws IOException {
	    long logIndex = repState.getLogEnd() + 1;

	    try {
	    	storage.appendOperation(logIndex, term, tokens);
	    	
	    } catch (IOException e) {
	    	throw e;
	    }

	    repState.incrementLogEnd();
	    
	    try {
	        storage.saveRepState(repState.getRepState());
	    } catch (IOException e) {
	        try {
	            storage.saveRepState(repState.getRepState());
	        } catch (IOException retryFailure) {
	            System.err.println("FATAL: Could not persist replication metadata.");
	            System.exit(1);
	        }
	    }

	    return logIndex;
	}
	
	public synchronized List<Response> applyCommittedThrough()
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
	                    Arrays.copyOfRange(operation, 2, operation.length)
	                )
	            );
	        }
	    }

	    return responses;
	}

	private Response applyCommittedOperation(String[] tokens) 
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
		} else if (input[0].equalsIgnoreCase("reset") || input[0].equalsIgnoreCase("no_op")) {
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
	
	public boolean requiresSnapshotCatchUp(long followerLogEnd) throws IOException {
	    long snapshotBoundary = storage.getSnapshotBoundary();

	    return followerLogEnd < snapshotBoundary;
	}
	
	public String[] prepareSnapshotTransfer() throws IOException {
		String[] transferSnapshot = new String[2];
		
		transferSnapshot[0] = "" + storage.getSnapshotBoundary();
		transferSnapshot[1] = storage.loadSerialisedSnapshot();
		
		return transferSnapshot;
		
	}
	
	public Long[] retrieveBoundaryIndexAndTerm() throws IOException {
		Long[] boundaries = new Long[1];
		
		boundaries[0] = storage.getSnapshotBoundary();
		
		return boundaries;
	}
	
	public void installSnapshotCatchUp(String serialisedSnapshot, long boundary) throws IOException {
		Map<String, String> snapshot = storage.deserialiseSnapshot(serialisedSnapshot);
		
		storage.writeSnapshot(snapshot, boundary);
		
		state.installSnapshot(snapshot);
		
		repState.installSnapshotBaseline(boundary);
	}
	
	public synchronized void persistElecState(String[] elecState) throws IOException {
		storage.saveElecState(elecState);
	}
	
	public synchronized long retrieveLastLogTerm() throws IOException {
		List<String[]> log = storage.loadLog();
		
		if (log.isEmpty()) {
		    return 0L;
		}
		
		String[] lastEntry = log.get(log.size()-1);
		
		return Long.parseLong(lastEntry[1]);
	}
	
	public synchronized long retrieveLogTerm(Long index) throws IOException {
		List<String[]> log = storage.loadLog();
		long term = -1l;
		
		for (String[] entry : log) {
			if (Long.parseLong(entry[0]) != index) {
				continue;
			}
			
			term = Long.parseLong(entry[1]);
			break;
		}
		
		if (term != -1l) {
			return term;
		} else {
			throw new IllegalStateException(
				    "WAL entry not found at index: " + index
				);
		}
	}
	
	public synchronized String[] retrieveEntry(Long index) throws IOException {
		List<String[]> log = storage.loadLog();
		String[] tokens = null;
		
		for (String[] entry : log) {
			if (Long.parseLong(entry[0]) != index) {
				continue;
			}
			
			tokens = entry;
			break;
		}
		
		if (tokens != null) {
			return tokens;
		} else {
			throw new IllegalStateException(
				    "WAL entry not found at index: " + index
				);
		}
	}
	
	public synchronized void truncateUncommittedFrom(long index) throws IOException {
		if (index <= repState.getCommitIndex()) {
		    throw new IllegalStateException(
		        "Cannot truncate committed log entry at index " + index
		    );
		}
		
		storage.truncateLogFrom(index);
		repState.retreatLogEnd(index-1);
	}
}
