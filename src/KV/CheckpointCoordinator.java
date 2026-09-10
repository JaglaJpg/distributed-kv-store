package kv;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class CheckpointCoordinator {
	private boolean signal = false;
	private boolean checkpointRequested = false;
	private final AtomicInteger mutationCount = new AtomicInteger();
	private final ReentrantReadWriteLock globalLock = new ReentrantReadWriteLock(true);
	
	public CheckpointCoordinator(int count) {
		this.mutationCount.set(count);
	}

	private synchronized void triggerSignal() {
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
	
	public void incrementMutationCount() {
		mutationCount.getAndIncrement();
		
		if(mutationCount.intValue() >= 1000) {
			triggerSignal();
		}
	}
	
	public void resetMutationCount() {
		mutationCount.set(0);
	}
	
	public synchronized void requestCheckpoint() {
		if(!checkpointRequested) {
			checkpointRequested = true;
		}
	}

	public synchronized void finishCheckpoint(boolean success) {
	    if (success) {
	        resetMutationCount();
	    }

	    checkpointRequested = false;
	    notifyAll();
	}

	
	public synchronized void enterOperation() throws InterruptedException {
		while(checkpointRequested) {
			wait();
		}
		
		globalLock.readLock().lock();
	}

	public void leaveOperation() {
		globalLock.readLock().unlock();
	}

	public void enterCheckpoint() {
		globalLock.writeLock().lock();
	}

	public void leaveCheckpoint() {
		globalLock.writeLock().unlock();
	}
	
	
}
