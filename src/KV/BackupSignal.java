package kv;

public class BackupSignal {
	private boolean signal = false;

	public synchronized void triggerSignal() {
		signal = true;
		notify();
	}

	public synchronized void waitForSignal() {
		while (!signal) {
			try {
				wait();
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				return;
			}

			
		}
		signal = false;
	}
}
