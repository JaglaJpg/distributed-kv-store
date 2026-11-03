package kv.state;

public class LockState {
	public int readers = 0;
	public boolean writer = false;
}
