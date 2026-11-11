package kv.cluster;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import kv.protocol.ExecutionStatus;
import kv.protocol.OperationalStatus;
import kv.protocol.Response;

public class ClusterManager {

	private final String nodeId;
	private volatile NodeRole role;
	private volatile OperationalStatus opStatus;

	private Map<String, PeerConnection> peers = new ConcurrentHashMap<>();
	private String leaderId;

	private record ForwardedRequest(
			long requestId,
			PeerConnection originPeer
			) {}

	private Map<Long, CompletableFuture<Response>> pendingRequests =
			new ConcurrentHashMap<>();
	private Map<Long, ForwardedRequest> pendingForwardedRequests = new ConcurrentHashMap<>();

	private AtomicLong nextRequestId = new AtomicLong(0);

	private long catchUpInProgressThrough = -1;

	private final OperationProcessor operationProcessor;
	private final ReplicationState repState;
	private final ElectionState elecState;

	private final Object commitLock = new Object();
	private final Object catchUpLock = new Object();
	
	private final Random rand = new Random();
	private volatile long electionDeadline = System.nanoTime() + 
			TimeUnit.MILLISECONDS.toNanos(rand.nextInt(1,301));
	
	private Set<String> myVotes = new HashSet<>();

	public ClusterManager(
			String nodeId,
			String successorId,
			NodeRole role,
			OperationProcessor operationProcessor,
			ReplicationState repState,
			ElectionState elecState) {

		this.nodeId = nodeId;
		this.role = role;
		this.operationProcessor = operationProcessor;
		this.repState = repState;
		this.opStatus = (role == NodeRole.LEADER)? OperationalStatus.READY : OperationalStatus.SYNCING;
		this.elecState = elecState;
	}

	public void sendHello(PeerConnection peer) {
		if (role == NodeRole.LEADER) {
			peer.queueMessage("HELLO " + nodeId + " LEADER");
			peer.queueMessage("SYNC_STATUS " + repState.getCommitIndex());
		} else {
			peer.queueMessage("HELLO " + nodeId + " FOLLOWER");
		}
		
	}
	
	public void sendHeartbeat() {
		sendToFollowers("HEARTBEAT " + elecState.getCurrentTerm() + " " + nodeId);
	}

	public void handleMessage(String msg, PeerConnection peer) throws IOException, InterruptedException {

		if (msg.startsWith("HELLO ")) {
			handleHello(msg, peer);
			return;
		}

		if (role == NodeRole.LEADER) {
			handleAsLeader(msg, peer);
		} else if (role == NodeRole.FOLLOWER) {
			handleAsFollower(msg, peer);
		}

	}

	public Response handleClientMutation(String[] tokens) throws InterruptedException, ExecutionException, IOException {
		if (opStatus == OperationalStatus.SYNCING) {
			return new Response(ExecutionStatus.NOT_READY, "This Node is synchronising, retry shortyly");
		}
		
		long requestId = nextRequestId.getAndIncrement();

		CompletableFuture<Response> future = new CompletableFuture<>();

		pendingRequests.put(requestId, future);

		if (role != NodeRole.LEADER) {
			String message = "FORWARD " + requestId + " " + String.join(" ", tokens);
			sendToLeader(message);

		} else {
			long logIndex = 0l;
			long entryTerm = 0l;
			
			try {
				entryTerm = elecState.getCurrentTerm();

				logIndex = operationProcessor.persistOperation(entryTerm, tokens);
				
			} catch (IOException e) {
			    pendingRequests.remove(requestId);
			    return new Response(
			        ExecutionStatus.ERR_STORAGE,
			        "Write failed due to a storage error."
			    );
			}
			

			pendingForwardedRequests.put(
					logIndex,
					new ForwardedRequest(
							requestId,
							null
							)
					);

			String message = "REPLICATE " + entryTerm + " " + logIndex + " " + entryTerm + " " + String.join(" ", tokens);

			sendToFollowers(message);

		}

		try {
		    return future.get(5, TimeUnit.SECONDS);
		} catch (TimeoutException e) {
		    return new Response(
		        ExecutionStatus.ERR_TIMEOUT,
		        "Write timed out; commit status unknown."
		    );
		} finally {
		    pendingRequests.remove(requestId, future);
		}
		
	}

	public void sendToLeader(String message) {
	    PeerConnection leader = peers.get(leaderId);

	    if (leader != null) {
	        leader.queueMessage(message);
	    }
	}

	public void sendToFollowers(String message) {
		if(role == NodeRole.FOLLOWER) {
			return;
		}

		for (PeerConnection follower : peers.values()) {
			follower.queueMessage(message);
		}
	}

	private void handleHello(String msg, PeerConnection peer) {
		String[] parts = msg.split(" ");

		String peerId = parts[1];
		NodeRole peerRole = NodeRole.valueOf(parts[2]);

		peers.put(peerId, peer);

		if (peerRole == NodeRole.LEADER) {
			leaderId = peerId;
		}
	}

	public void handleAsLeader(String msg, PeerConnection peer) throws IOException, InterruptedException {

		if (msg.startsWith("FORWARD ")) {
			String[] parts = msg.split(" ", 3);

			long requestId = Long.parseLong(parts[1]);
			String[] tokens = parts[2].split("\\s+");
			
			long logIndex = 0l;
			long entryTerm = 0l;
			
			try {
				entryTerm = elecState.getCurrentTerm();
				
				logIndex = operationProcessor.persistOperation(entryTerm, tokens);
				
			} catch (IOException e) {
				peer.queueMessage(
					    "RESULT " + requestId + " ERR_STORAGE Write failed due to a storage error."
					);
				return;
			}

			pendingForwardedRequests.put(
					logIndex,
					new ForwardedRequest(
							requestId,
							peer
							)
					);

			String message = "REPLICATE " + entryTerm + " " + logIndex + " " + entryTerm + " " + String.join(" ", tokens);

			sendToFollowers(message);

		} else if (msg.startsWith("REPLICATE_ACK ")) {
			String[] parts = msg.split(" ");
			long ackedIndex = Long.parseLong(parts[1]);

			synchronized (commitLock) {
				if (ackedIndex <= repState.getCommitIndex()) {
					peer.queueMessage("COMMIT " + repState.getCommitIndex());
					return;
				}

				long oldCommitIndex = repState.getCommitIndex();

				if (ackedIndex > oldCommitIndex && ackedIndex <= repState.getLogEnd()) {
				    repState.advanceCommitIndex(ackedIndex);
				}

				long newCommitIndex = repState.getCommitIndex();

				if (newCommitIndex > oldCommitIndex) {
					List<Response> responses = operationProcessor.applyCommittedThrough(); 

					int responseIndex = 0;

					for (long logIndex = oldCommitIndex + 1;
							logIndex <= newCommitIndex;
							logIndex++) {

						Response response = responses.get(responseIndex++);

						ForwardedRequest request =
								pendingForwardedRequests.remove(logIndex);

						if (request.originPeer() == null) {

							CompletableFuture<Response> future =
									pendingRequests.remove(request.requestId());

							if (future != null) {
							    future.complete(response);
							}

						} else {

							String payload = response.getPayload();

							String message =
									"RESULT "
											+ request.requestId() + " "
											+ response.getStatus().name() + " "
											+ (payload == null ? "" : payload);

							request.originPeer().queueMessage(message);
						}
					}

				}


				sendToFollowers("COMMIT " + newCommitIndex);
			}

		} else if (msg.startsWith("CATCHUP_REQUEST ")) {
			String[] parts = msg.split(" ");
			String reason = parts[1];
			Long followerLogEnd = Long.parseLong(parts[2]);
			Long target = Long.parseLong(parts[3]);
			List<String[]> catchUp;
			
			if(operationProcessor.requiresSnapshotCatchUp(followerLogEnd)) {
				
		        String[] snapshotTransfer =
		                operationProcessor.prepareSnapshotTransfer();

		        long snapshotBoundary =
		                Long.parseLong(snapshotTransfer[0]);

		        String serializedSnapshot =
		                snapshotTransfer[1];

		        peer.queueMessage(
		                "CATCHUP SNAPSHOT "
		                + snapshotBoundary + " "
		                + serializedSnapshot
		        );

		        catchUp = operationProcessor.loadCatchUpFromLog(
		                snapshotBoundary,
		                target
		        );
				
			} else {
				catchUp = operationProcessor.loadCatchUpFromLog(followerLogEnd, target);
				
			}
			
			for(String[] operation : catchUp) {
				peer.queueMessage("CATCHUP " + reason +" "+ target + " " + String.join(" ", operation));
			}

			peer.queueMessage("CATCHUP_DONE " + reason +" "+ target);

			

		} else {
			System.err.println(
					"Unknown cluster message received by leader: " + msg
					);

		}
	}

	public void handleAsFollower(String msg, PeerConnection peer) throws IOException, InterruptedException {
		if (msg.startsWith("REPLICATE ")) {
			Long logEnd = repState.getLogEnd();

			String[] parts = msg.split(" ", 5);

			long leaderTerm = Long.parseLong(parts[1]);
			long logIndex = Long.parseLong(parts[2]);
			long entryTerm = Long.parseLong(parts[3]);
			String[] tokens = parts[4].split(" ");

			if (logEnd < logIndex - 1) {

				synchronized (catchUpLock) {
					if (logIndex > catchUpInProgressThrough) {
						catchUpInProgressThrough = logIndex;

						sendToLeader(
								"CATCHUP_REQUEST REPLICATE "
										+ logEnd + " " + logIndex
								);
					}
				}

			} else if (logEnd >= logIndex) {
				sendToLeader("REPLICATE_ACK " + logIndex);
				return;

			} else {
				operationProcessor.persistOperation(entryTerm, tokens);
				sendToLeader("REPLICATE_ACK " + logIndex);
			}

		} else if (msg.startsWith("COMMIT ")) {

			long commitIndex = repState.getCommitIndex();
			long logEnd = repState.getLogEnd();

			long newCommitIndex =
					Long.parseLong(msg.split(" ")[1]);

			if (logEnd < newCommitIndex) {

			    synchronized (catchUpLock) {
			        if (newCommitIndex > catchUpInProgressThrough) {
			            catchUpInProgressThrough = newCommitIndex;

			            sendToLeader(
			                "CATCHUP_REQUEST COMMIT "
			                + logEnd + " " + newCommitIndex
			            );
			        }
			    }

			} else if (newCommitIndex > commitIndex) {

			    repState.advanceCommitIndex(newCommitIndex);
			    operationProcessor.applyCommittedThrough();
			}

		} else if (msg.startsWith("RESULT ")) {
			String[] parts = msg.split(" ", 4);

			Long requestId = Long.parseLong(parts[1]);

			ExecutionStatus status =
					ExecutionStatus.valueOf(parts[2]);

			String payload =
					parts.length == 4 ? parts[3] : null;

			Response response = new Response(status, payload);

			CompletableFuture<Response> future =
					pendingRequests.remove(requestId);

			if (future != null) {
			    future.complete(response);
			}

		} else if (msg.startsWith("CATCHUP ")) {
			String[] parts = msg.split(" ", 5);
			
			if (parts[1].equals("SNAPSHOT")) {
				Long boundary = Long.parseLong(parts[2]);
				String serialisedSnapshot = parts[3];
				
				operationProcessor.installSnapshotCatchUp(serialisedSnapshot, boundary);
				
			} else {
				Long incomingIndex = Long.parseLong(parts[3]);
				Long logEnd = repState.getLogEnd();
				
				String[] entry = parts[4].split(" ", 2);

				long entryTerm = Long.parseLong(entry[0]);
				String[] tokens = entry[1].split(" ");

				if (incomingIndex == logEnd + 1) {
					operationProcessor.persistOperation(entryTerm, tokens);
				} 
			}


		} else if (msg.startsWith("SYNC_STATUS")) {
			Long leaderCommitIndex = Long.parseLong(msg.split("\\s+")[1]);
			Long myCommitIndex = repState.getCommitIndex();
			Long myLogEnd = repState.getLogEnd();
			
			if (myLogEnd < leaderCommitIndex) {

			    synchronized (catchUpLock) {
			        catchUpInProgressThrough = leaderCommitIndex;
			    }

			    sendToLeader("CATCHUP_REQUEST SYNCING " + myLogEnd + " " + leaderCommitIndex);

			} else {

			    if (myCommitIndex < leaderCommitIndex) {
			        repState.advanceCommitIndex(leaderCommitIndex);
			        operationProcessor.applyCommittedThrough();
			    }

			    opStatus = OperationalStatus.READY;
			}
			
			
		} else if (msg.startsWith("CATCHUP_DONE ")) {
			String[] parts = msg.split(" ");

			String reason = parts[1];
			Long target = Long.parseLong(parts[2]);
			Long logEnd = repState.getLogEnd();

			if (logEnd < target) {
				sendToLeader("CATCHUP_REQUEST " + reason + " " + logEnd + " " + target);

			} else {
				if (reason.equals("REPLICATE")) {
					sendToLeader("REPLICATE_ACK " + target);

				} else if (reason.equals("COMMIT")) {
					if (target > repState.getCommitIndex()) {
						repState.advanceCommitIndex(target);
						operationProcessor.applyCommittedThrough();

					}
				} else if (reason.equals("SYNCING")) {
					if (target > repState.getCommitIndex()) {
						repState.advanceCommitIndex(target);
						operationProcessor.applyCommittedThrough();
						
					}
					opStatus = OperationalStatus.READY;
					
				}
				synchronized (catchUpLock) {
				    if (target == catchUpInProgressThrough) {
				        catchUpInProgressThrough = -1;
				    }
				}

			}




		} else {
			System.err.println(
					"Unknown cluster message received by leader: " + msg
					);
		}
	}

	public void removePeer(PeerConnection peer) {
	    peers.entrySet().removeIf(entry -> {
	        if (entry.getValue() == peer) {
	            String nodeId = entry.getKey();
	            
	            if (nodeId.equals(leaderId)) {
	            	opStatus = OperationalStatus.SYNCING;
	            }
	            
	            return true;
	        }
	        return false;
	    });
	}
	
	public boolean isReady() {
		return opStatus == OperationalStatus.READY;
	}
	
	public NodeRole getNodeRole() {
		return role;
	}
	
	public boolean hasTimedOut() {
		return System.nanoTime() >= electionDeadline;
	}

	public void startElection() throws IOException {
		role = NodeRole.CANDIDATE;
		
		Long term = elecState.startNewTerm();
		String votedFor = elecState.voteFor(nodeId);
		
		myVotes = new HashSet<>();
		myVotes.add(votedFor);
		
		operationProcessor.persistElecState(elecState.getElectionState());
		
		electionDeadline = System.nanoTime() + 
				TimeUnit.MILLISECONDS.toNanos(rand.nextInt(1,301));
		
		for (PeerConnection peer : peers.values()) {
			if ( peer != null) {
				peer.queueMessage("REQUEST_VOTE " + term + " ");
			}
		}
		
		//TO BE CONTINUED
		
		
	}
	
}
