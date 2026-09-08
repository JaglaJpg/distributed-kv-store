package KV;

public interface Command {
	Response execute(String[] tokens);
}
