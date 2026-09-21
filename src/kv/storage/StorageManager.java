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
import java.util.regex.Pattern;

public class StorageManager {

	private static final Path DATA_DIRECTORY = Path.of("data");
	private static final Path LOG_PATH = DATA_DIRECTORY.resolve("operations.log");
	private static final Path SNAPSHOT_PATH = DATA_DIRECTORY.resolve("snapshot.dat");
	private static final Path TEMP_SNAPSHOT_PATH = DATA_DIRECTORY.resolve("snapshot.tmp");

	private long checkNum;

	public StorageManager() throws IOException {
		Files.createDirectories(DATA_DIRECTORY);
		Files.deleteIfExists(TEMP_SNAPSHOT_PATH);

		if (Files.notExists(LOG_PATH)) {
			Files.createFile(LOG_PATH);
		}
		
		byte[] bytes = Files.readAllBytes(LOG_PATH);
		
		String content = new String(bytes, StandardCharsets.UTF_8);

		if (!content.isEmpty() && !content.endsWith(System.lineSeparator())) {
		    int lastSeparator = content.lastIndexOf(System.lineSeparator());

		    String validPart = "";

		    if (lastSeparator != -1) {
		        validPart =
		            content.substring(
		                0,
		                lastSeparator + System.lineSeparator().length()
		            );
		    }

		    try (FileChannel channel = FileChannel.open(
		            LOG_PATH,
		            StandardOpenOption.WRITE,
		            StandardOpenOption.TRUNCATE_EXISTING)) {

		        ByteBuffer buffer =
		            ByteBuffer.wrap(validPart.getBytes(StandardCharsets.UTF_8));

		        while (buffer.hasRemaining()) {
		            channel.write(buffer);
		        }

		        channel.force(true);
		    }
		}

		long snapshotNum = 0;
		long logNum = 0;

		if (Files.exists(SNAPSHOT_PATH)) {
			try (BufferedReader br = Files.newBufferedReader(SNAPSHOT_PATH)) {
				String firstLine = br.readLine();

				if (firstLine != null) {
					snapshotNum = Long.parseLong(
							Pattern.compile(" ").splitAsStream(firstLine).toList().getLast()
							);
				}
			}
		}

		try (BufferedReader br = Files.newBufferedReader(LOG_PATH)) {
			String line;
			String lastLine = null;

			while ((line = br.readLine()) != null) {
				lastLine = line;
			}

			if (lastLine != null) {
				logNum = Long.parseLong(lastLine.split(" ")[0]);
			}
		}

		this.checkNum = Math.max(snapshotNum, logNum);
	}


	public Snapshot loadSnapshot() throws IOException {
		Map<String, String> snapshotMap = new HashMap<>();
		Long latestApplied = 0l;

		if (Files.notExists(SNAPSHOT_PATH)) {
			return new Snapshot(snapshotMap, 0l);
		}

		try (FileReader fr = new FileReader(SNAPSHOT_PATH.toFile());
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

	    try (BufferedReader reader = Files.newBufferedReader(LOG_PATH)) {
	        String line;

	        while ((line = reader.readLine()) != null) {
	            if (line.isBlank()) {
	                continue;
	            }

	            int index = 0;

	            int checkNumSpace = line.indexOf(' ', index);
	            String operationNum = line.substring(index, checkNumSpace);
	            index = checkNumSpace + 1;

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
	                            command
	                    });
	                }

	                case "DELETE" -> {
	                    ParsedField key = parseField(line, index);

	                    operations.add(new String[] {
	                            operationNum,
	                            command,
	                            key.value()
	                    });
	                }

	                case "PUT" -> {
	                    ParsedField key = parseField(line, index);
	                    ParsedField value = parseField(line, key.nextIndex());

	                    operations.add(new String[] {
	                            operationNum,
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

	public synchronized void appendOperation(String[] tokens) throws IOException {
	    long operationNum = ++checkNum;
	    String command = tokens[0].toUpperCase();

	    String operation = operationNum + " " + command;

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
	            LOG_PATH,
	            StandardOpenOption.CREATE,
	            StandardOpenOption.WRITE,
	            StandardOpenOption.APPEND)) {

	        ByteBuffer buffer =
	                ByteBuffer.wrap(record.getBytes(StandardCharsets.UTF_8));

	        while (buffer.hasRemaining()) {
	            channel.write(buffer);
	        }

	        channel.force(true);
	    }
	}

	public void writeSnapshot(Map<String, String> data) throws IOException {
		try (FileChannel channel = FileChannel.open(
				TEMP_SNAPSHOT_PATH,
				StandardOpenOption.CREATE,
				StandardOpenOption.WRITE,
				StandardOpenOption.TRUNCATE_EXISTING )) {

			ByteBuffer checkNumLine =
					ByteBuffer.wrap(
							("snapshot last applied = " + checkNum + System.lineSeparator())
							.getBytes(StandardCharsets.UTF_8)
							);

			while (checkNumLine.hasRemaining()) {
				channel.write(checkNumLine);
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
				TEMP_SNAPSHOT_PATH, 
				SNAPSHOT_PATH, 
				StandardCopyOption.REPLACE_EXISTING, 
				StandardCopyOption.ATOMIC_MOVE
				);

		try (FileChannel channel = FileChannel.open(
				LOG_PATH, 
				StandardOpenOption.WRITE)) {
			channel.truncate(0);
			channel.force(true);
		}
	}

	public void resetPersistance() throws IOException {
	    Files.deleteIfExists(SNAPSHOT_PATH);

	    try (FileChannel channel = FileChannel.open(
	            LOG_PATH,
	            StandardOpenOption.WRITE)) {

	        channel.truncate(0);
	        channel.force(true);
	    }
	}

}
