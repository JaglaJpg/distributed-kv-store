package kv;

public interface Command {
	Response execute(String[] tokens);
}
