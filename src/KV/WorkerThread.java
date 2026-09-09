package kv;

import java.io.IOException;

public class WorkerThread extends Thread {
	private KVSharedState mySharedState;
	private BackupSignal backup;
	
	public WorkerThread(KVSharedState mySharedState, BackupSignal backup) {
		this.mySharedState = mySharedState;
		this.backup = backup;
		setName("My-System-Worker"); 
		setDaemon(true);
	}

	@Override
	public void run() {
		while (true) {
			backup.waitForSignal();
			
			mySharedState.enterGlobalStateForBackup();
			
			try {
				mySharedState.createSnapshot();
				mySharedState.completeCheckpoint();
				
			} catch (IOException e) {
				e.printStackTrace();
				mySharedState.failCheckpoint();
				
			} finally {
				mySharedState.leaveGlobalStateFromBackup();
			}
		}
	}
}
