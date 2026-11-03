package kv.server;

import kv.state.StateManager;

public class WorkerThread extends Thread {
	private StateManager stateManager;
	
	public WorkerThread(StateManager stateManager) {
		this.stateManager = stateManager;
		setName("My-System-Worker"); 
		setDaemon(true);
	}

	@Override
	public void run() {
		while (true) {
			stateManager.waitForCheckpointSignal();
			stateManager.performCheckpoint();
		}
	}
}
