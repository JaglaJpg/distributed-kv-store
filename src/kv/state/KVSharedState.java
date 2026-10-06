package kv.state;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import kv.protocol.Command;
import kv.protocol.ExecutionStatus;
import kv.protocol.Response;
import kv.storage.Snapshot;

public class KVSharedState {
	private Map<String, String> store;
	private final Map<String, Command> commands = new HashMap<>();
	private Map<String, LockState> locks = new HashMap<>();

	public KVSharedState() throws IOException {
		commands.put("PUT", (tokens) ->{
			String key = tokens[1];
			String value = tokens[2];

			Response response;

			String entry = store.get(key);

			if(entry != null) {
				response = new Response(ExecutionStatus.OK_UPDATED);
			} else {
				response = new Response(ExecutionStatus.OK_ADDED);

			}

			store.put(key, value);
			return response;
		});

		commands.put("GET", (tokens) ->{
			String key = tokens[1];
			String value = store.get(key);
			if(value != null) {
				return new Response(ExecutionStatus.OK_SUCCESS, value);
			} else {
				return new Response(ExecutionStatus.ERR_NOT_FOUND);
			}

		});

		commands.put("DELETE", (tokens) ->{
			String key = tokens[1];
			String status = store.remove(key);

			if(status != null) {
				return new Response(ExecutionStatus.OK_DELETED);
			} else {
				return new Response(ExecutionStatus.ERR_NOT_FOUND);
			}
		});
		
		commands.put("RESET", (tokens) ->{
			this.store = new ConcurrentHashMap<>();
			
			return new Response(ExecutionStatus.OK_SUCCESS, "Successfully Reset Data Store");
		});
	}

	int initialiseState(Snapshot snapshot, List<String[]> operations, Long lastApplied) throws IOException {
	    long latestApplied = snapshot.getCheckNum();
	    long resetCheckNum = 0;

	    this.store = new ConcurrentHashMap<>(snapshot.getSnapshot());

	    for (int i = operations.size() - 1; i >= 0; i--) {
	        String[] op = operations.get(i);
	        long opNum = Long.parseLong(op[0]);
	        
	        if (opNum > lastApplied) {
	            continue;
	        }

	        if (opNum <= latestApplied) {
	            break;
	        }

	        if (op[1].equalsIgnoreCase("RESET")) {
	            this.store = new ConcurrentHashMap<>();
	            resetCheckNum = opNum;
	            break;
	        }
	    }

	    long recoveryBoundary = Math.max(latestApplied, resetCheckNum);

	    int replayedOperations = 0;

	    for (String[] tokens : operations) {
	        long opNum = Long.parseLong(tokens[0]);

	        if (opNum <= recoveryBoundary) {
	            continue;
	        }

	        if (opNum > lastApplied) {
	            continue;
	        }

	        executeCommand(Arrays.copyOfRange(tokens, 1, tokens.length));
	        replayedOperations++;
	    }

	    return replayedOperations;
	}

	public synchronized boolean acquireLock(String key, String command) throws InterruptedException {
		Thread me = Thread.currentThread();

		while (true) {

			String value = store.get(key);

			// Key currently doesn't exist
			if (value == null && !command.equalsIgnoreCase("PUT")) {
				return false;
			}

			LockState lock = locks.computeIfAbsent(key, k -> new LockState());

			if (command.equalsIgnoreCase("PUT") || command.equalsIgnoreCase("DELETE")) {

				if (!lock.writer && lock.readers == 0) {
					lock.writer = true;

					System.out.println(me.getName() + " got a " + key + " writer lock!");
					return true;
				}

			} else {

				if (!lock.writer) {
					lock.readers++;

					System.out.println(me.getName() + " got a " + key + " reader lock!");
					return true;
				}
			}

			System.out.println(
					me.getName() + " waiting to get a lock as someone else is accessing..."
					);

			wait();
		}
	}

	public synchronized void releaseLock(String key, String command) {
		LockState lock = locks.get(key);

		if (lock == null) {
			notifyAll(); 
			return;
		}

		if (command.equalsIgnoreCase("GET")) {
			lock.readers--;
		} else {
			lock.writer = false;
		}

		notifyAll(); 
	}

	public Response executeCommand(String[] tokens) throws IOException {
		Command cmd = commands.get(tokens[0].toUpperCase());
		return cmd.execute(tokens);
	}

	public Map<String, String> copyStore(){
		return new HashMap<>(store);
	}

	public void replaceState(Map<String, String> snapshot) {
		this.store = new ConcurrentHashMap<>(snapshot);
	}
}
