package kv;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
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
        String operation = tokens[0] + " ";

        operation += tokens[1].length() + ":" + tokens[1];

        if (tokens[0].equalsIgnoreCase("PUT")) {
            operation += tokens[2].length() + ":" + tokens[2];
        }

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
    			
    			int index = 0;

    			int colon = line.indexOf(':', index);
    			int keyLength = Integer.parseInt(line.substring(index, colon));
    			index = colon + 1;

    			String key = line.substring(index, index + keyLength);
    			index += keyLength;

    			colon = line.indexOf(':', index);
    			int valueLength = Integer.parseInt(line.substring(index, colon));
    			index = colon + 1;

    			String value = line.substring(index, index + valueLength);
    			
    			snapshot.put(key, value);
            }
    		
    	}
    	
    	return snapshot;
    }
    
    public List<String[]> loadLog() throws IOException {
        List<String[]> operations = new ArrayList<>();

        try (FileReader fr = new FileReader(LOG_PATH.toFile());
             BufferedReader reader = new BufferedReader(fr)) {

            String line;

            while ((line = reader.readLine()) != null) {

                if (line.trim().isEmpty()) {
                    continue;
                }

                int index = 0;

                int space = line.indexOf(' ', index);
                String command = line.substring(index, space);
                index = space + 1;

                int colon = line.indexOf(':', index);
                int keyLength = Integer.parseInt(line.substring(index, colon));
                index = colon + 1;

                String key = line.substring(index, index + keyLength);
                index += keyLength;

                if (command.equalsIgnoreCase("PUT")) {

                    colon = line.indexOf(':', index);
                    int valueLength = Integer.parseInt(line.substring(index, colon));
                    index = colon + 1;

                    String value = line.substring(index, index + valueLength);

                    operations.add(new String[] {
                        command,
                        key,
                        value
                    });

                } else if (command.equalsIgnoreCase("DELETE")) {

                    operations.add(new String[] {
                        command,
                        key
                    });
                }
            }
        }

        return operations;
    }
    
    public void writeSnapshot(Map<String, String> data) throws IOException {
    	try (FileWriter fw = new FileWriter(TEMP_SNAPSHOT_PATH.toFile());
   	         BufferedWriter writer = new BufferedWriter(fw)) {
    		
    		for (Map.Entry<String, String> entry : data.entrySet()) {
    			String key = entry.getKey();
    			String value = entry.getValue();
    		    writer.write(key.length() + ":" + key + value.length() + ":" + value);
    		    writer.newLine();
    		}
    	}
    	
    	Files.move(
		        TEMP_SNAPSHOT_PATH, 
		        SNAPSHOT_PATH, 
		        StandardCopyOption.REPLACE_EXISTING, 
		        StandardCopyOption.ATOMIC_MOVE
		    );
    	
    	Files.writeString(
    		    LOG_PATH,
    		    "",
    		    StandardOpenOption.TRUNCATE_EXISTING
    		);
    }
	

}
