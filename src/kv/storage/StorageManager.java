package kv.storage;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
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

    private static final String LOG_FILE = "operations.log";
    private static final String SNAPSHOT_FILE = "snapshot.dat";
    private static final String TEMP_SNAPSHOT_FILE = "snapshot.tmp";
    private static final String REP_STATE_FILE = "replication.meta";
    private static final String TEMP_LOG_FILE = "operations.tmp";
    private static final String TEMP_REP_STATE_FILE = "replication.tmp";
    private static final String ELEC_STATE_FILE = "election.meta";
    private static final String TEMP_ELEC_STATE_FILE = "election.tmp";

    private final Path nodeDir;
    private final Path logPath;
    private final Path snapshotPath;
    private final Path tempSnapshotPath;
    private final Path repStatePath;
    private final Path tempLogPath;
    private final Path tempRepStatePath;
    private final Path elecStatePath;
    private final Path tempElecStatePath;

    public StorageManager(String nodeId) throws IOException {

        this.nodeDir = DATA_DIRECTORY.resolve(nodeId);

        this.logPath = nodeDir.resolve(LOG_FILE);
        this.snapshotPath = nodeDir.resolve(SNAPSHOT_FILE);
        this.tempSnapshotPath = nodeDir.resolve(TEMP_SNAPSHOT_FILE);
        this.repStatePath = nodeDir.resolve(REP_STATE_FILE);
        this.tempLogPath = nodeDir.resolve(TEMP_LOG_FILE);
        this.tempRepStatePath = nodeDir.resolve(TEMP_REP_STATE_FILE);
        this.elecStatePath = nodeDir.resolve(ELEC_STATE_FILE);
        this.tempElecStatePath = nodeDir.resolve(TEMP_ELEC_STATE_FILE);

        Files.createDirectories(nodeDir);
        
        Files.deleteIfExists(tempSnapshotPath);
        Files.deleteIfExists(tempLogPath);
        Files.deleteIfExists(tempRepStatePath);
        Files.deleteIfExists(tempElecStatePath);

        if (Files.notExists(logPath)) {
            Files.createFile(logPath);
        }

        byte[] bytes = Files.readAllBytes(logPath);

        String content = new String(bytes, StandardCharsets.UTF_8);

        if (!content.isEmpty()
                && !content.endsWith(System.lineSeparator())) {

            int lastSeparator =
                    content.lastIndexOf(System.lineSeparator());

            String validPart = "";

            if (lastSeparator != -1) {
                validPart =
                        content.substring(
                                0,
                                lastSeparator
                                + System.lineSeparator().length()
                        );
            }

            try (FileChannel channel = FileChannel.open(
                    logPath,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.TRUNCATE_EXISTING)) {

                ByteBuffer buffer =
                        ByteBuffer.wrap(
                                validPart.getBytes(StandardCharsets.UTF_8)
                        );

                while (buffer.hasRemaining()) {
                    channel.write(buffer);
                }

                channel.force(true);
            }
        }
    }


	public Snapshot loadSnapshot() throws IOException {
		Map<String, String> snapshotMap = new HashMap<>();
		Long latestApplied = 0l;

		if (Files.notExists(snapshotPath)) {
			return new Snapshot(snapshotMap, 0l);
		}

		try (FileReader fr = new FileReader(snapshotPath.toFile());
				BufferedReader reader = new BufferedReader(fr)) {

			String firstLine = reader.readLine();
			String[] firstLineArray = firstLine.split(" ");
			latestApplied = Long.parseLong(firstLineArray[firstLineArray.length - 1]);

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

				snapshotMap.put(key, value);
			}

		}

		return new Snapshot(snapshotMap, latestApplied);
	}

	private record ParsedField(String value, int nextIndex) {}

	private ParsedField parseField(String line, int index) {
		int colon = line.indexOf(':', index);
		int length = Integer.parseInt(line.substring(index, colon));

		int valueStart = colon + 1;
		int valueEnd = valueStart + length;

		return new ParsedField(
				line.substring(valueStart, valueEnd),
				valueEnd
				);
	}

	public List<String[]> loadLog() throws IOException {
		List<String[]> operations = new ArrayList<>();

		try (BufferedReader reader = Files.newBufferedReader(logPath)) {
			String line;

			while ((line = reader.readLine()) != null) {
				if (line.isBlank()) {
					continue;
				}

				int index = 0;

				int checkNumSpace = line.indexOf(' ', index);
				String operationNum = line.substring(index, checkNumSpace);
				index = checkNumSpace + 1;

				// Read the term
				int termSpace = line.indexOf(' ', index);
				String term = line.substring(index, termSpace);
				index = termSpace + 1;

				int commandSpace = line.indexOf(' ', index);
				String command;

				if (commandSpace == -1) {
					command = line.substring(index);
				} else {
					command = line.substring(index, commandSpace);
					index = commandSpace + 1;
				}

				switch (command.toUpperCase()) {
				case "RESET" -> {
					operations.add(new String[] {
							operationNum,
							term,
							command
					});
				}

				case "DELETE" -> {
					ParsedField key = parseField(line, index);
					operations.add(new String[] {
							operationNum,
							term,
							command,
							key.value()
					});
				}

				case "PUT" -> {
					ParsedField key = parseField(line, index);
					ParsedField value = parseField(line, key.nextIndex());
					operations.add(new String[] {
							operationNum,
							term,
							command,
							key.value(),
							value.value()
					});
				}
				}
			}
		}

		return operations;
	}

	
	public synchronized void appendOperation(long operationNum, long term, String[] tokens) throws IOException {
		String command = tokens[0].toUpperCase();

		String operation = operationNum + " " + term + " " + command;

		switch (command) {
		case "PUT" -> {
			operation += " "
					+ tokens[1].length() + ":" + tokens[1]
							+ tokens[2].length() + ":" + tokens[2];
		}

		case "DELETE" -> {
			operation += " "
					+ tokens[1].length() + ":" + tokens[1];
		}

		}

		String record = operation + System.lineSeparator();

		try (FileChannel channel = FileChannel.open(
		        logPath,
		        StandardOpenOption.CREATE,
		        StandardOpenOption.WRITE)) {

		    long originalSize = channel.size();
		    channel.position(originalSize);

		    try {
		        ByteBuffer buffer = ByteBuffer.wrap(
		                record.getBytes(StandardCharsets.UTF_8)
		        );

		        while (buffer.hasRemaining()) {
		            channel.write(buffer);
		        }

		        channel.force(true);

		    }catch (IOException appendFailure) {
		            try {
		                channel.truncate(originalSize);
		                channel.force(true);
		            } catch (IOException rollbackFailure) {
		                System.err.println(
		                    "FATAL: WAL rollback failed: " + rollbackFailure.getMessage()
		                );
		                System.exit(1);
		            }

		            throw appendFailure;
		        }
		}
	}

	public synchronized void writeSnapshot(Map<String, String> data, long lastApplied) throws IOException {
		try (FileChannel channel = FileChannel.open(
				tempSnapshotPath,
				StandardOpenOption.CREATE,
				StandardOpenOption.WRITE,
				StandardOpenOption.TRUNCATE_EXISTING )) {

			ByteBuffer snapshotBoundaryLine =
					ByteBuffer.wrap(
							("snapshot last applied = " + lastApplied + System.lineSeparator())
							.getBytes(StandardCharsets.UTF_8)
							);

			while (snapshotBoundaryLine.hasRemaining()) {
				channel.write(snapshotBoundaryLine);
			}


			for (Map.Entry<String, String> entry : data.entrySet()) {
				String key = entry.getKey();
				String value = entry.getValue();
				String record = key.length() + ":" + key + value.length() + ":" + value + System.lineSeparator();

				ByteBuffer buffer = 
						ByteBuffer.wrap(record.getBytes(StandardCharsets.UTF_8));

				while (buffer.hasRemaining()) {
					channel.write(buffer);
				}

			}
			channel.force(true);
		}

		Files.move(
			    tempSnapshotPath,
			    snapshotPath,
			    StandardCopyOption.REPLACE_EXISTING,
			    StandardCopyOption.ATOMIC_MOVE
			);

			compactLogThrough(lastApplied);
	}

	public synchronized void saveRepState(Long[] repState) throws IOException {

		try (FileChannel channel = FileChannel.open(
				tempRepStatePath,
				StandardOpenOption.CREATE,
				StandardOpenOption.WRITE,
				StandardOpenOption.TRUNCATE_EXISTING)) {

			for (Long index : repState) {
				String line = index + System.lineSeparator();

				ByteBuffer buffer = ByteBuffer.wrap(line.getBytes(StandardCharsets.UTF_8));

				while (buffer.hasRemaining()) {
					channel.write(buffer);
				}

			}

			channel.force(true);
		}
		
		Files.move(
				tempRepStatePath, 
				repStatePath, 
				StandardCopyOption.REPLACE_EXISTING, 
				StandardCopyOption.ATOMIC_MOVE
				);

	}

	public Long[] loadRepState() throws IOException {

	    if (Files.notExists(repStatePath)) {
	        return new Long[] {0L, 0L, 0L};
	    }

	    Long[] repState = new Long[3];
	    int i = 0;

	    try (BufferedReader reader = Files.newBufferedReader(repStatePath)) {
	        String line;

	        while ((line = reader.readLine()) != null) {

	            if (line.isBlank()) {
	                continue;
	            }

	            if (i >= repState.length) {
	                throw new IOException("Invalid replication state file");
	            }

	            repState[i] = Long.parseLong(line.trim());
	            i++;
	        }
	    }

	    if (i != 3) {
	        throw new IOException("Invalid replication state file");
	    }

	    return repState;
	}
	
	public Long getSnapshotBoundary() throws IOException {
		Long boundary = 0L;
		
		if (Files.notExists(snapshotPath)) {
		    return boundary;
		}
		
		try (BufferedReader br = Files.newBufferedReader(snapshotPath)) {
			String[] line = br.readLine().split("\\s+");
			
			boundary = Long.parseLong(line[4]);
		}
		
		return boundary;
	}
	
	public String loadSerialisedSnapshot() throws IOException {
		Map<String, String> data = loadSnapshot().getSnapshot();
		
	    StringBuilder snapshot = new StringBuilder();

	    for (Map.Entry<String, String> entry : data.entrySet()) {
	        String key = entry.getKey();
	        String value = entry.getValue();

	        snapshot.append(key.length())
	                .append(":")
	                .append(key)
	                .append(value.length())
	                .append(":")
	                .append(value);
	        
	    }

	    return snapshot.toString();
	}
	
	public Map<String, String> deserialiseSnapshot(String serialisedSnapshot) {
		Map<String, String> snapshotMap = new HashMap<>();
	
		int index = 0;
		
		while (index < serialisedSnapshot.length()) {

			int colon = serialisedSnapshot.indexOf(':', index);
			int keyLength = Integer.parseInt(serialisedSnapshot.substring(index, colon));
			index = colon + 1;

			String key = serialisedSnapshot.substring(index, index + keyLength);
			index += keyLength;

			colon = serialisedSnapshot.indexOf(':', index);
			int valueLength = Integer.parseInt(serialisedSnapshot.substring(index, colon));
			index = colon + 1;

			String value = serialisedSnapshot.substring(index, index + valueLength);
			index += valueLength;

			snapshotMap.put(key, value);
		}
		
		return snapshotMap;
	}
	
	public synchronized void saveElecState(String[] elecState) throws IOException {
		try (FileChannel channel = FileChannel.open(
				tempElecStatePath,
				StandardOpenOption.CREATE,
				StandardOpenOption.WRITE,
				StandardOpenOption.TRUNCATE_EXISTING)) {

			for (String index : elecState) {
				String line = index + System.lineSeparator();

				ByteBuffer buffer = ByteBuffer.wrap(line.getBytes(StandardCharsets.UTF_8));

				while (buffer.hasRemaining()) {
					channel.write(buffer);
				}

			}

			channel.force(true);
		}
		
		Files.move(
				tempElecStatePath, 
				elecStatePath, 
				StandardCopyOption.REPLACE_EXISTING, 
				StandardCopyOption.ATOMIC_MOVE
				);

	}
	
	public synchronized String[] loadElecState() throws IOException {
		
		if (Files.notExists(elecStatePath)) {
			return new String[] {"0", null};
		}
		
		String [] elecState = new String[2];
		int i = 0;
		
		try (BufferedReader reader = Files.newBufferedReader(elecStatePath)) {
			String line;
			
			while ((line = reader.readLine()) != null) {
				
				if (line.isBlank()) {
	                continue;
	            }
				
				if (i >= elecState.length) {
	                throw new IOException("Invalid election state file");
	            }
				
				if (line.equals("null")) {
					line = null;
				}
				
				elecState[i] = line == null ? null : line.trim();
	            i++;
			}
		}
		
		if (i != 2) {
	        throw new IOException("Invalid election state file");
	    }

	    return elecState;
		
	}
	
	public synchronized void truncateLogFrom(long index) throws IOException {
		try (FileChannel channel = FileChannel.open(
				tempLogPath, 
				StandardOpenOption.CREATE,
				StandardOpenOption.WRITE,
				StandardOpenOption.TRUNCATE_EXISTING )) {
			
			try (BufferedReader reader = Files.newBufferedReader(logPath)) {
				String line;

				while ((line = reader.readLine()) != null) {
					if (line.isBlank()) {
						continue;
					}
					
					if(Long.parseLong(line.split(" ")[0]) >= index) {
						continue;
					
					} else {
						String record = line + System.lineSeparator();

						ByteBuffer buffer =
						        ByteBuffer.wrap(record.getBytes(StandardCharsets.UTF_8));
						
						while (buffer.hasRemaining()) {
							channel.write(buffer);
						}
					}
					
				}
			}
			
			channel.force(true);
		}
		
		Files.move(
				tempLogPath, 
				logPath, 
				StandardCopyOption.REPLACE_EXISTING, 
				StandardCopyOption.ATOMIC_MOVE
				);
	}
	
	public synchronized void compactLogThrough(long index) throws IOException {
	    try (FileChannel channel = FileChannel.open(
	            tempLogPath,
	            StandardOpenOption.CREATE,
	            StandardOpenOption.WRITE,
	            StandardOpenOption.TRUNCATE_EXISTING)) {

	        try (BufferedReader reader = Files.newBufferedReader(logPath)) {
	            String line;

	            while ((line = reader.readLine()) != null) {
	                if (line.isBlank()) {
	                    continue;
	                }

	                if (Long.parseLong(line.split(" ")[0]) <= index) {
	                    continue;
	                }

	                ByteBuffer buffer = ByteBuffer.wrap(
	                    (line + System.lineSeparator()).getBytes(StandardCharsets.UTF_8)
	                );

	                while (buffer.hasRemaining()) {
	                    channel.write(buffer);
	                }
	            }
	        }

	        channel.force(true);
	    }

	    Files.move(
	        tempLogPath,
	        logPath,
	        StandardCopyOption.REPLACE_EXISTING,
	        StandardCopyOption.ATOMIC_MOVE
	    );
	}
}
