package kv;

public class LockState {
	public int readers = 0;
	public boolean writer = false;
}
