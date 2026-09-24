package kv.server;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import kv.cluster.ClusterManager;
import kv.cluster.ClusterReaderThread;
import kv.cluster.ClusterWriterThread;
import kv.cluster.NodeRole;
import kv.cluster.OperationProcessor;
import kv.cluster.PeerConnection;
import kv.cluster.ReplicationState;
import kv.state.StateManager;
import kv.storage.StorageManager;

public class KVNodeC {

    private static final String NODE_ID = "Casper-3";
    private static final int CLIENT_PORT = 7003;
    private static final int MY_CLUSTER_PORT = 8003;
    private static final int NEXT_CLUSTER_PORT = 8002;

    public static Socket prevNodeSocket = null;
    public static Socket nextNodeSocket = null;

    public static void main(String[] args) throws IOException {

        ServerSocket kvServerSocket = null;
        boolean listening = true;

        StorageManager storageManager = new StorageManager(NODE_ID);

        StateManager stateManager = new StateManager(storageManager);

        Long[] savedRepState = storageManager.loadRepState();
        ReplicationState repState = new ReplicationState(savedRepState);

        OperationProcessor operationProcessor =
                new OperationProcessor(
                        repState,
                        storageManager,
                        stateManager
                );

        ClusterManager clusterManager =
                new ClusterManager(
                        NODE_ID,
                        NodeRole.FOLLOWER,
                        operationProcessor,
                        repState
                );

        new WorkerThread(stateManager).start();

        try {
            kvServerSocket = new ServerSocket(CLIENT_PORT);
        } catch (IOException e) {
            System.err.println(
                    "Could not start " + NODE_ID + " specified port."
            );
            System.exit(-1);
        }

        System.out.println(NODE_ID + " started");

        new Thread(() -> {
            try (ServerSocket clusterListener =
                         new ServerSocket(MY_CLUSTER_PORT)) {

                System.out.println(
                        NODE_ID
                        + " listening for previous node on port "
                        + MY_CLUSTER_PORT
                );

                while (true) {
                    prevNodeSocket = clusterListener.accept();

                    PeerConnection peer = new PeerConnection();

                    System.out.println(
                            "Previous node connected to " + NODE_ID
                    );

                    new ClusterReaderThread(
                            prevNodeSocket,
                            peer,
                            clusterManager
                    ).start();

                    new ClusterWriterThread(
                            prevNodeSocket,
                            peer
                    ).start();

                    clusterManager.sendHello(peer);
                }

            } catch (IOException e) {
                System.err.println(
                        "Cluster listener error on "
                        + NODE_ID + ": " + e.getMessage()
                );
            }
        }).start();

        new Thread(() -> {

            System.out.println(
                    NODE_ID
                    + " attempting to connect to next node on port "
                    + NEXT_CLUSTER_PORT
            );

            while (nextNodeSocket == null) {
                try {
                    nextNodeSocket =
                            new Socket(
                                    "localhost",
                                    NEXT_CLUSTER_PORT
                            );

                    PeerConnection peer = new PeerConnection();

                    System.out.println(
                            NODE_ID
                            + " successfully connected to next node!"
                    );

                    new ClusterReaderThread(
                            nextNodeSocket,
                            peer,
                            clusterManager
                    ).start();

                    new ClusterWriterThread(
                            nextNodeSocket,
                            peer
                    ).start();

                    clusterManager.sendHello(peer);

                } catch (IOException e) {
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException ignored) {
                    }
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