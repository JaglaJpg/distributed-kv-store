package kv;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StorageManager {
	
    private static final Path DATA_DIRECTORY = Path.of("data");
    private static final Path LOG_PATH = DATA_DIRECTORY.resolve("operations.log");
    private static final Path SNAPSHOT_PATH = DATA_DIRECTORY.resolve("snapshot.dat");
    private static final Path TEMP_SNAPSHOT_PATH = DATA_DIRECTORY.resolve("snapshot.tmp");
	
    public StorageManager() throws IOException {
        Files.createDirectories(DATA_DIRECTORY);

        if (Files.notExists(LOG_PATH)) {
            Files.createFile(LOG_PATH);
        }
    }
    
    public synchronized void appendOperation(String[] tokens) throws IOException {
    	String operation = String.join(" " , tokens);
    	
    	try (FileWriter fw = new FileWriter(LOG_PATH.toFile(), true);
    	         BufferedWriter writer = new BufferedWriter(fw)) {
    		
    		writer.write(operation);
    		writer.newLine();
    		
    	}
    }
    
    public Map<String, String> loadSnapshot() throws IOException {
    	Map<String, String> snapshot = new HashMap<>();
    	
    	if (Files.notExists(SNAPSHOT_PATH)) {
    	    return snapshot;
    	}
    	
    	try (FileReader fr = new FileReader(SNAPSHOT_PATH.toFile());
   	         BufferedReader reader = new BufferedReader(fr)) {
    		
    		String line;
    		while ((line = reader.readLine()) != null) {
                
    			if (line.trim().isEmpty()) {
                    continue; 
                }
    			
    			String[] entry = line.split(":");
    			
    			snapshot.put(entry[0], entry[1]);
            }
    		
    	}
    	
    	return snapshot;
    }
    
    public List<String[]> loadLog() throws IOException {
    	List<String[]> operations = new ArrayList<>();
    	
    	try (FileReader fr = new FileReader(LOG_PATH.toFile());
      	         BufferedReader reader = new BufferedReader(fr)) {
    		
    		String line;
    		while((line = reader.readLine()) != null) {
    			
    			if (line.trim().isEmpty()) {
                    continue; 
                }
    			
    			String[] operation = line.split(" ");
    			
    			operations.add(operation);
    		}
    		
    	}
    	
    	return operations;
    }
    
    public void writeSnapshot(Map<String, String> data) throws IOException {
    	try (FileWriter fw = new FileWriter(TEMP_SNAPSHOT_PATH.toFile());
   	         BufferedWriter writer = new BufferedWriter(fw)) {
    		
    		for (Map.Entry<String, String> entry : data.entrySet()) {
    		    writer.write(entry.getKey() + ":" + entry.getValue());
    		    writer.newLine();
    		}
    	}
    	
    	Files.move(
		        TEMP_SNAPSHOT_PATH, 
		        SNAPSHOT_PATH, 
		        StandardCopyOption.REPLACE_EXISTING, 
		        StandardCopyOption.ATOMIC_MOVE
		    );
    }
	

}
