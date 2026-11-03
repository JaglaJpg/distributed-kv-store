package kv.protocol;

public interface Command {
	Response execute(String[] tokens);
}
