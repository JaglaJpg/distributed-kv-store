package kv.server;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;

import kv.cluster.ClusterManager;
import kv.cluster.ClusterReaderThread;
import kv.cluster.ClusterWriterThread;
import kv.cluster.ElectionState;
import kv.cluster.ElectionThread;
import kv.cluster.OperationProcessor;
import kv.cluster.PeerConnection;
import kv.cluster.ReplicationState;
import kv.state.StateManager;
import kv.storage.StorageManager;

public class KVNodeB {

    private static final String NODE_ID = "Balthasar-2";
    private static final String SUCCESSOR_ID = "Melchior-1";
	private static final String PREDECESSOR_ID = "Casper-3";
    private static final int CLIENT_PORT = 7002;
    private static final int MY_CLUSTER_PORT = 8002;
    private static final int NEXT_CLUSTER_PORT = 8001;

    public static Socket nextNodeSocket = null;

    public static void main(String[] args) throws IOException, InterruptedException {

        ServerSocket kvServerSocket = null;
        boolean listening = true;

        StorageManager storageManager = new StorageManager(NODE_ID);

        Long[] savedRepState = storageManager.loadRepState();
        ReplicationState repState = new ReplicationState(savedRepState);
		
		String[] savedElecState = storageManager.loadElecState();
		ElectionState elecState = new ElectionState(savedElecState);

        List<String[]> log = storageManager.loadLog();

        long snapshotBoundary = storageManager.getSnapshotBoundary();

        // Validate WAL continuity
        long expectedIndex = snapshotBoundary + 1;

        for (String[] entry : log) {
            long index = Long.parseLong(entry[0]);

            if (index != expectedIndex) {
                throw new IOException(
                    "WAL gap: expected " + expectedIndex + ", found " + index
                );
            }

            expectedIndex++;
        }

        // Last valid index, or snapshot boundary if WAL is empty
        long latestIndex = expectedIndex - 1;

        if (latestIndex < repState.getLogEnd()) {
            throw new IOException("Replication metadata ahead of WAL");
        }

        if (latestIndex > repState.getLogEnd()) {
            while (repState.getLogEnd() < latestIndex) {
                repState.incrementLogEnd();
            }

            storageManager.saveRepState(repState.getRepState());
        }

        StateManager stateManager = new StateManager(storageManager, repState);

        OperationProcessor operationProcessor =
                new OperationProcessor(
                    repState,
                    storageManager,
                    stateManager
                );

        if (repState.getLastApplied() < repState.getCommitIndex()) {
            operationProcessor.applyCommittedThrough();
        }

    	ClusterManager clusterManager =
				new ClusterManager(
						NODE_ID,
						operationProcessor,
						repState,
						elecState
						);

        new WorkerThread(stateManager).start();
        new ElectionThread(clusterManager).start();

        try {
            kvServerSocket = new ServerSocket(CLIENT_PORT);
        } catch (IOException e) {
            System.err.println(
                "Could not start " + NODE_ID + " specified port."
            );
            System.exit(-1);
        }

        System.out.println(NODE_ID + " started");

        // Incoming cluster connections
        new Thread(() -> {
            try (ServerSocket clusterListener =
                    new ServerSocket(MY_CLUSTER_PORT)) {

                System.out.println(
                    NODE_ID + " listening for previous node on port "
                    + MY_CLUSTER_PORT
                );

                while (true) {
                    Socket acceptedSocket = clusterListener.accept();
                    PeerConnection peer = new PeerConnection(PREDECESSOR_ID);

                    System.out.println(
                        "Previous node connected to " + NODE_ID
                    );

                    new Thread(() -> {
                        try {
                            ClusterReaderThread reader =
                                    new ClusterReaderThread(
                                        acceptedSocket,
                                        peer,
                                        clusterManager
                                    );

                            ClusterWriterThread writer =
                                    new ClusterWriterThread(
                                        acceptedSocket,
                                        peer
                                    );

                            reader.start();
                            writer.start();

                            clusterManager.sendHello(peer);

                            reader.join();
                            writer.interrupt();

                        } catch (IOException e) {
                            System.out.println(
                                NODE_ID + " lost connection with previous node"
                            );

                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;

                        } finally {
                            try {
                                acceptedSocket.close();
                            } catch (IOException e) {
                                System.err.println(
                                    NODE_ID
                                    + " error closing previous-node socket: "
                                    + e.getMessage()
                                );
                            }

                            clusterManager.removePeer(peer);
                        }

                    }).start();
                }

            } catch (IOException e) {
                System.err.println(
                    "Cluster listener error on "
                    + NODE_ID + ": " + e.getMessage()
                );
            }
        }).start();

        // Outgoing cluster connection and reconnection
        new Thread(() -> {

            while (true) {
                PeerConnection peer = null;

                try {
                    System.out.println(
                        NODE_ID + " attempting to connect to next node on port "
                        + NEXT_CLUSTER_PORT
                    );

                    nextNodeSocket =
                            new Socket("localhost", NEXT_CLUSTER_PORT);

                    peer = new PeerConnection(SUCCESSOR_ID);

                    System.out.println(
                        NODE_ID + " successfully connected to next node!"
                    );

                    ClusterReaderThread reader =
                            new ClusterReaderThread(
                                nextNodeSocket,
                                peer,
                                clusterManager
                            );

                    ClusterWriterThread writer =
                            new ClusterWriterThread(
                                nextNodeSocket,
                                peer
                            );

                    reader.start();
                    writer.start();

                    clusterManager.sendHello(peer);

                    reader.join();
                    writer.interrupt();

                    System.out.println(
                        NODE_ID + " connection to next node was lost."
                    );

                } catch (IOException e) {
                    System.out.println(
                        NODE_ID
                        + " could not connect to next node. Retrying..."
                    );

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;

                } finally {
                    if (nextNodeSocket != null) {
                        try {
                            nextNodeSocket.close();
                        } catch (IOException e) {
                            System.err.println(
                                "Error closing outgoing socket: "
                                + e.getMessage()
                            );
                        }

                        nextNodeSocket = null;
                    }

                    if (peer != null) {
                        clusterManager.removePeer(peer);
                    }
                }

                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }

        }).start();

        while (listening) {
            new KVClientThread(
                kvServerSocket.accept(),
                stateManager,
                operationProcessor,
                clusterManager
            ).start();

            System.out.println(
                "New " + NODE_ID + " thread started"
            );
        }

        kvServerSocket.close();
    }
}
