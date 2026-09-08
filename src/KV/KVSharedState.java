package KV;

import java.util.HashMap;
import java.util.Map;

public class KVSharedState {
	private Map<String, Entry> myStore;
	private final Map<String, Command> commands = new HashMap<String, Command>();
	
	public KVSharedState(Map<String, Entry> store) {
		this.myStore = store;
		
		commands.put("PUT", (tokens) ->{
			String key = tokens[1];
			String value = tokens[2];
			
			Response response;
			
			Entry entry = myStore.get(key);
			
			if(entry.value != null) {
				response = new Response(ExecutionStatus.OK_UPDATED);
			} else {
				response = new Response(ExecutionStatus.OK_ADDED);
			}
			
			
			entry.value = value;
			
			return response;
		});
		
		commands.put("GET", (tokens) ->{
			String key = tokens[1];
			Entry status = myStore.get(key);
			if(status != null) {
				return new Response(ExecutionStatus.OK_SUCCESS, status.value);
			} else {
				return new Response(ExecutionStatus.ERR_NOT_FOUND);
			}
			
		});
		
		commands.put("DELETE", (tokens) ->{
			String key = tokens[1];
			Entry status = myStore.remove(key);
			
			if(status != null) {
				return new Response(ExecutionStatus.OK_DELETED);
			} else {
				return new Response(ExecutionStatus.ERR_NOT_FOUND);
			}
		});
		
	}
	
	public synchronized boolean aquireLock(String key, String type, String command)
	        throws InterruptedException {

	    Thread me = Thread.currentThread();

	    while (true) {

	        Entry entry = myStore.get(key);

	        // Key currently doesn't exist
	        if (entry == null) {

	            // PUT is allowed to create it
	            if (command.equalsIgnoreCase("PUT")) {
	                entry = new Entry();
	                entry.writer = true;
	                myStore.put(key, entry);

	                System.out.println(me.getName() + " created and locked " + key);
	                return true;
	            }

	            // GET / DELETE on nonexistent key
	            return false;
	        }

	        if (type.equalsIgnoreCase("writer")) {

	            if (!entry.writer && entry.readers == 0) {
	                entry.writer = true;

	                System.out.println(me.getName() + " got a " + key + " writer lock!");
	                return true;
	            }

	        } else {

	            if (!entry.writer) {
	                entry.readers++;

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

	public synchronized void releaseLock(String key, String type) {
		Entry entry = myStore.get(key);
		
		if(entry == null) {
			notifyAll(); 
			return;
		}
		
		if(type.equalsIgnoreCase("writer")) {
			entry.writer = false;
		} else {
			entry.readers--;
		}
		
		notifyAll(); 
	}
	
	public String processCommand(String[] tokens) {
		 Command cmd = commands.get(tokens[0].toUpperCase());
		 
		 return cmd.execute(tokens).toNetworkString();
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
	

}
