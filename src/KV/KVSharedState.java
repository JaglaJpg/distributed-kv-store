package kv;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class KVSharedState {
	private Map<String, String> store;
	private final Map<String, Command> commands = new HashMap<>();
	private Map<String, LockState> locks = new HashMap<>();
	private final StorageManager manager;
	private final ReentrantReadWriteLock globalLock = new ReentrantReadWriteLock(true);
	private BackupSignal backup;

	private final AtomicInteger mutationCount;
	private boolean checkpointRequested = false;

	public KVSharedState(StorageManager manager, BackupSignal backup) throws IOException {
		Map<String, String> snapshot = manager.loadSnapshot();
		Map<String, String> store = new ConcurrentHashMap<>(snapshot);

		this.manager= manager;
		this.store = store;
		this.backup = backup;

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

		this.mutationCount = new AtomicInteger(initialiseState());

	}

	private int initialiseState() throws IOException {
		List<String[]> operations = manager.loadLog();

		for (String[] tokens : operations) {
			executeCommand(tokens);
		}

		return operations.size();
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

	private Response executeCommand(String[] tokens) {
		Command cmd = commands.get(tokens[0].toUpperCase());
		return cmd.execute(tokens);
	}

	public Response processCommand(String[] tokens) throws IOException {
		if (!tokens[0].equalsIgnoreCase("GET")) {
			manager.appendOperation(tokens);
			mutationCount.incrementAndGet();
		}

		if(mutationCount.intValue() >= 3) {
			requestCheckpoint();
		}

		return executeCommand(tokens);
	}

	public boolean validateInput(String[] input) {
		if (input == null || input.length == 0) {
			return false;
		}

		if (input[0].equalsIgnoreCase("delete") || input[0].equalsIgnoreCase("get")) {
			return input.length == 2;
		} else if (input[0].equalsIgnoreCase("put")) {
			return input.length == 3;
		}

		return false;
	}

	public void createSnapshot() throws IOException {
		manager.writeSnapshot(store);
	}

	public synchronized void enterGlobalState() {
		while(checkpointRequested) {
			try {
				wait();
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				return;
			}
		}
		globalLock.readLock().lock();
	}

	public void leaveGlobalState() {
		globalLock.readLock().unlock();
	}

	public void enterGlobalStateForBackup() {
		globalLock.writeLock().lock();
	}

	// Called by the Worker Thread when the backup is finished
	public void leaveGlobalStateFromBackup() {
		globalLock.writeLock().unlock();
	}

	public synchronized void requestCheckpoint() {
		if(!checkpointRequested) {
			checkpointRequested = true;
			backup.triggerSignal();
		}
	}


	public synchronized void completeCheckpoint() {
		mutationCount.set(0);
		checkpointRequested = false;
		notifyAll();
	}
	
	public synchronized void failCheckpoint() {
	    checkpointRequested = false;
	    notifyAll();
	}

}
