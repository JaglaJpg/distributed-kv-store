package kv.cluster;

import java.io.IOException;
import java.util.Arrays;
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
	private final Object electionLock = new Object();

	private final Random rand = new Random();
	private volatile long electionDeadline = System.nanoTime() + 
			TimeUnit.MILLISECONDS.toNanos(rand.nextInt(800, 1501));

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
			peer.queueMessage("SYNC_STATUS " + elecState.getCurrentTerm()
			+ " " + nodeId + " " + repState.getCommitIndex());
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

		if (msg.startsWith("HEARTBEAT ") ||
				msg.startsWith("REQUEST_VOTE ") ||
				msg.startsWith("VOTE_RESPONSE ")) {
			handleElection(msg, peer);
			return;
		}

		if (msg.startsWith("REPLICATE ") ||
				msg.startsWith("COMMIT ") ||
				msg.startsWith("SYNC_STATUS ") ||
				msg.startsWith("CATCHUP ") ||
				msg.startsWith("CATCHUP_DONE ") ||
				msg.startsWith("RESULT ")) {

			handleAsFollower(msg, peer);
			return;
		}

		handleAsLeader(msg, peer);

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

			long prevEntryTerm = logIndex == 1
					? 0L
							: operationProcessor.retrieveLogTerm(logIndex - 1);

			String message = "REPLICATE " + entryTerm + " " + nodeId + " " + prevEntryTerm + " " + logIndex + " " + entryTerm + " " + String.join(" ", tokens);

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
		if(role != NodeRole.LEADER) {
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
	}

	public void handleAsLeader(String msg, PeerConnection peer) throws IOException, InterruptedException {
		if (role != NodeRole.LEADER) {
			return;
		}

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
						"RESULT " + entryTerm + " " + nodeId + " " + requestId + " ERR_STORAGE Write failed due to a storage error."
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

			long prevEntryTerm = logIndex == 1
					? 0L
							: operationProcessor.retrieveLogTerm(logIndex - 1);

			String message = "REPLICATE " + entryTerm + " " + nodeId + " " + prevEntryTerm + " " + logIndex + " " + entryTerm + " " + String.join(" ", tokens);

			sendToFollowers(message);

		} 
		else if (msg.startsWith("REPLICATE_ACK ")) {

		    // ========================================
		    // SECTION 1: PARSE & VALIDATE ACK
		    // EXISTING CODE
		    // ========================================

		    String[] parts = msg.split(" ");

		    long ackTerm = Long.parseLong(parts[1]);
		    long ackedIndex = Long.parseLong(parts[2]);

		    long myTerm = elecState.getCurrentTerm();

		    if (ackTerm != myTerm) {
		        return;
		    }


		    // ========================================
		    // SECTION 2: REPLICATION PROGRESS
		    // NEW — YOUR SPACE
		    // ========================================

		    if (ackedIndex < repState.getLogEnd()) {
		        String[] nextEntry = operationProcessor.retrieveEntry(ackedIndex+1);
		        String[] entry = operationProcessor.retrieveEntry(ackedIndex); 
		        
		        peer.queueMessage("REPLICATE " + myTerm + " " + nodeId + " " 
		        		+ entry[1] + " " + String.join(" ", nextEntry));
		    }
		    
		    // TODO:
		    // Does this follower need another entry?
		    //
		    // If yes:
		    //   - Find the next entry
		    //   - Find its preceding entry's term
		    //   - Send REPLICATE to this follower only
		    //
		    // Keep this independent of commit handling.





		    // ========================================
		    // SECTION 3: COMMIT HANDLING
		    // EXISTING CODE
		    // ========================================

		    synchronized (commitLock) {

		        if (ackedIndex <= repState.getCommitIndex()) {
		            peer.queueMessage(
		                "COMMIT " + myTerm + " " + nodeId + " "
		                + repState.getCommitIndex()
		            );
		            return;
		        }

		        long oldCommitIndex = repState.getCommitIndex();

		        if (ackedIndex > oldCommitIndex
		                && ackedIndex <= repState.getLogEnd()
		                && operationProcessor.retrieveLogTerm(ackedIndex) == myTerm) {

		            repState.advanceCommitIndex(ackedIndex);
		        }

		        long newCommitIndex = repState.getCommitIndex();


		        // ====================================
		        // SECTION 4: APPLY & RESPOND
		        // EXISTING CODE
		        // ====================================

		        if (newCommitIndex > oldCommitIndex) {

		            List<Response> responses =
		                    operationProcessor.applyCommittedThrough();

		            int responseIndex = 0;

		            for (long logIndex = oldCommitIndex + 1;
		                    logIndex <= newCommitIndex;
		                    logIndex++) {

		                Response response =
		                        responses.get(responseIndex++);

		                ForwardedRequest request =
		                        pendingForwardedRequests.remove(logIndex);

		                if (request != null && response != null) {

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
		                                + myTerm + " "
		                                + nodeId + " "
		                                + request.requestId() + " "
		                                + response.getStatus().name() + " "
		                                + (payload == null ? "" : payload);

		                        request.originPeer().queueMessage(message);
		                    }
		                }
		            }
		        }


		        // ====================================
		        // SECTION 5: NOTIFY FOLLOWERS
		        // EXISTING CODE
		        // ====================================

		        sendToFollowers(
		            "COMMIT " + myTerm + " " + nodeId + " "
		            + newCommitIndex
		        );
		    }


		} else if (msg.startsWith("REPLICATE_REJECT ")) {
			String[] parts = msg.split("\\s+");

			long leaderTerm = Long.parseLong(parts[1]);
			long logIndex = Long.parseLong(parts[2]);

			if (leaderTerm != elecState.getCurrentTerm()) {
				return;
			}

			if (logIndex <= 2) {
				String[] entry = operationProcessor.retrieveEntry(1l);

				peer.queueMessage("REPLICATE " + elecState.getCurrentTerm() + " " + nodeId 
						+ " 0 " 
						+ String.join(" ", entry));

			} else if(logIndex -1 <= operationProcessor.retrieveBoundaryIndexAndTerm()[0]) {
				String[] snapshotTransfer =
						operationProcessor.prepareSnapshotTransfer();

				long snapshotBoundary =
						Long.parseLong(snapshotTransfer[0]);

				String serializedSnapshot =
						snapshotTransfer[1];

				peer.queueMessage(
						"CATCHUP " + elecState.getCurrentTerm() + " " + nodeId 
						+ " SNAPSHOT "
						+ snapshotBoundary + " "
						+ serializedSnapshot
						);

				List<String[]> catchUp = operationProcessor.loadCatchUpFromLog(
						snapshotBoundary,
						repState.getLogEnd()
						);

				for(String[] operation : catchUp) {
					peer.queueMessage("CATCHUP " + elecState.getCurrentTerm() + " " + nodeId + " SYNCING " + repState.getLogEnd() + " " + String.join(" ", operation));
				}

				peer.queueMessage("CATCHUP_DONE " + elecState.getCurrentTerm() + " " + nodeId  + " SYNCING " + repState.getLogEnd());

				return;

			} else {
				String[] entry = operationProcessor.retrieveEntry(logIndex-1);
				String[] prevEntry = operationProcessor.retrieveEntry(logIndex-2);

				peer.queueMessage("REPLICATE " + elecState.getCurrentTerm() + " " + nodeId 
						+ " " + prevEntry[1] + " " 
						+ String.join(" ", entry));

			}








		} else if (msg.startsWith("CATCHUP_REQUEST ")) {
			String[] parts = msg.split(" ");
			Long term = Long.parseLong(parts[1]);
			String reason = parts[2];
			Long followerLogEnd = Long.parseLong(parts[3]);
			Long target = Long.parseLong(parts[4]);
			List<String[]> catchUp;

			if (term != elecState.getCurrentTerm()) {
				return;
			}

			if(operationProcessor.requiresSnapshotCatchUp(followerLogEnd)) {

				String[] snapshotTransfer =
						operationProcessor.prepareSnapshotTransfer();

				long snapshotBoundary =
						Long.parseLong(snapshotTransfer[0]);

				String serializedSnapshot =
						snapshotTransfer[1];

				peer.queueMessage(
						"CATCHUP " + elecState.getCurrentTerm() + " " + nodeId 
						+ " SNAPSHOT "
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
				peer.queueMessage("CATCHUP " + elecState.getCurrentTerm() + " " + nodeId + " " + reason +" "+ target + " " + String.join(" ", operation));
			}

			peer.queueMessage("CATCHUP_DONE " + elecState.getCurrentTerm() + " " + nodeId + " " + reason + " " + target);



		} else {
			System.err.println(
					"Unknown cluster message received by leader: " + msg
					);

		}
	}

	public void handleAsFollower(String msg, PeerConnection peer) throws IOException, InterruptedException {
		if (msg.startsWith("REPLICATE ")) {
			Long logEnd = repState.getLogEnd();

			String[] parts = msg.split(" ", 7);

			long leaderTerm = Long.parseLong(parts[1]);
			String leaderId = parts[2];
			long prevEntryTerm = Long.parseLong(parts[3]);
			long logIndex = Long.parseLong(parts[4]);
			long prevEntryIndex = logIndex - 1;
			long entryTerm = Long.parseLong(parts[5]);
			String[] tokens = parts[6].split(" ");

			if (!acceptLeader(leaderTerm, leaderId)) {
				return;
			}

			if (logEnd < logIndex - 1) {

				// CASE 1: Missing previous entries
				synchronized (catchUpLock) {
					if (logIndex > catchUpInProgressThrough) {
						catchUpInProgressThrough = logIndex;

						sendToLeader(
								"CATCHUP_REQUEST " + elecState.getCurrentTerm()
								+ " REPLICATE " + logEnd + " " + logIndex
								);
					}
				}

			} else {

				// CASE 2: Validate previous entry
				try {
					Long myPrevEntryTerm;

					if (prevEntryIndex == 0L) {
						myPrevEntryTerm = 0L;

					} else if (prevEntryIndex == operationProcessor.retrieveBoundaryIndexAndTerm()[0]) {
						myPrevEntryTerm = operationProcessor.retrieveBoundaryIndexAndTerm()[1];
						// TODO: Snapshot boundary term support

					} else {
						myPrevEntryTerm = operationProcessor.retrieveLogTerm(prevEntryIndex);
					}

					if (myPrevEntryTerm != prevEntryTerm) {
						sendToLeader("REPLICATE_REJECT " + leaderTerm + " " + logIndex);
						return;
					}

				} catch (IllegalStateException e) {
					sendToLeader("REPLICATE_REJECT " + leaderTerm + " " + logIndex);
					return;
				}

				// CASE 3: Previous entry matches, current entry missing
				if (logIndex > logEnd) {

					operationProcessor.persistOperation(entryTerm, tokens);
					sendToLeader("REPLICATE_ACK " + leaderTerm + " " + logIndex);

				} else {

					// CASE 4: Current entry already exists
					long myEntryTerm = operationProcessor.retrieveLogTerm(logIndex);

					if (myEntryTerm == entryTerm) {

						// CASE 4A: Same entry, nothing to change
						sendToLeader("REPLICATE_ACK " + leaderTerm + " " + logIndex);

					} else {
						operationProcessor.truncateUncommittedFrom(logIndex);
						operationProcessor.persistOperation(entryTerm, tokens);
						sendToLeader("REPLICATE_ACK " + leaderTerm + " " + logIndex);

					}
				}
			}

		} else if (msg.startsWith("COMMIT ")) {

			long commitIndex = repState.getCommitIndex();
			long logEnd = repState.getLogEnd();

			String[] parts = msg.split("\\s+");

			long leaderTerm = Long.parseLong(parts[1]);
			String leaderId = parts[2];
			long newCommitIndex = Long.parseLong(parts[3]);

			if (!acceptLeader(leaderTerm, leaderId)) {
				return;
			}

			if (logEnd < newCommitIndex) {

				synchronized (catchUpLock) {
					if (newCommitIndex > catchUpInProgressThrough) {
						catchUpInProgressThrough = newCommitIndex;

						sendToLeader(
								"CATCHUP_REQUEST " + elecState.getCurrentTerm() + " COMMIT "
										+ logEnd + " " + newCommitIndex
								);
					}
				}

			} else if (newCommitIndex > commitIndex) {

				repState.advanceCommitIndex(newCommitIndex);
				operationProcessor.applyCommittedThrough();
			}

		} else if (msg.startsWith("RESULT ")) {
			String[] parts = msg.split(" ", 6);

			Long leaderTerm = Long.parseLong(parts[1]);
			String leaderId = parts[2];
			Long requestId = Long.parseLong(parts[3]);

			synchronized (electionLock) {
				if (leaderTerm != elecState.getCurrentTerm()
						|| role != NodeRole.FOLLOWER
						|| !leaderId.equals(this.leaderId)) {
					return;
				}
			}

			ExecutionStatus status =
					ExecutionStatus.valueOf(parts[4]);

			String payload =
					parts.length == 6 ? parts[5] : null;

			Response response = new Response(status, payload);

			CompletableFuture<Response> future =
					pendingRequests.remove(requestId);

			if (future != null) {
				future.complete(response);
			}

		} else if (msg.startsWith("CATCHUP ")) {
			String[] parts = msg.split(" ", 8);

			long leaderTerm = Long.parseLong(parts[1]);
			String leaderId = parts[2];

			if (!acceptLeader(leaderTerm, leaderId)) {
				return;
			}

			if (parts[3].equals("SNAPSHOT")) {
				Long boundary = Long.parseLong(parts[4]);
				String serialisedSnapshot = parts[5];

				operationProcessor.installSnapshotCatchUp(serialisedSnapshot, boundary);

			} else {
				Long incomingIndex = Long.parseLong(parts[5]);
				Long logEnd = repState.getLogEnd();

				long entryTerm = Long.parseLong(parts[6]);
				String[] tokens = parts[7].split(" ");

				if (incomingIndex == logEnd + 1) {
					operationProcessor.persistOperation(entryTerm, tokens);
				} 
			}


		} else if (msg.startsWith("SYNC_STATUS")) {
			String[] parts = msg.split("\\s+");

			Long leaderTerm = Long.parseLong(parts[1]);
			String leaderId = parts[2];
			Long leaderCommitIndex = Long.parseLong(parts[3]);
			Long myCommitIndex = repState.getCommitIndex();
			Long myLogEnd = repState.getLogEnd();

			if (!acceptLeader(leaderTerm, leaderId)) {
				return;
			}

			if (myLogEnd < leaderCommitIndex) {

				synchronized (catchUpLock) {
					catchUpInProgressThrough = leaderCommitIndex;
				}

				sendToLeader("CATCHUP_REQUEST " + elecState.getCurrentTerm() + " SYNCING " + myLogEnd + " " + leaderCommitIndex);

			} else {

				if (myCommitIndex < leaderCommitIndex) {
					repState.advanceCommitIndex(leaderCommitIndex);
					operationProcessor.applyCommittedThrough();
				}

				opStatus = OperationalStatus.READY;
			}


		} else if (msg.startsWith("CATCHUP_DONE ")) {
			String[] parts = msg.split(" ");

			Long leaderTerm = Long.parseLong(parts[1]);
			String leaderId = parts[2];
			String reason = parts[3];
			Long target = Long.parseLong(parts[4]);
			Long logEnd = repState.getLogEnd();

			if (!acceptLeader(leaderTerm, leaderId)) {
				return;
			}

			if (logEnd < target) {
				sendToLeader("CATCHUP_REQUEST "+ elecState.getCurrentTerm() + " " + reason + " " + logEnd + " " + target);

			} else {
				if (reason.equals("REPLICATE")) {
					sendToLeader("REPLICATE_ACK "+ elecState.getCurrentTerm() + " " + target);

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

	public void handleElection(String msg, PeerConnection peer) throws IOException {
		String[] parts = msg.split("\\s+");

		if (msg.startsWith("HEARTBEAT ")) {
			long leaderTerm = Long.parseLong(parts[1]);
			String leaderId = parts[2];

			if (!acceptLeader(leaderTerm, leaderId)) {
				return;
			}

		} else if (msg.startsWith("REQUEST_VOTE ")) {
			synchronized (electionLock) {
				long candidateTerm = Long.parseLong(parts[1]);
				long lastLogTerm = Long.parseLong(parts[2]);
				long lastLogIndex = Long.parseLong(parts[3]);
				String candidateId = parts[4];

				long myTerm = elecState.getCurrentTerm();
				long myLastLogTerm = operationProcessor.retrieveLastLogTerm();
				long myLogEnd = repState.getLogEnd();

				if (candidateTerm < myTerm) {
					peer.queueMessage("VOTE_RESPONSE " + myTerm + " NO " + nodeId);
					return;

				} else if (candidateTerm > myTerm) {
					stepDown();
					elecState.observeHigherTerm(candidateTerm);
					elecState.voteFor(null);
					operationProcessor.persistElecState(elecState.getElectionState());
				}

				String votedFor = elecState.getVotedFor();


				if ((votedFor != null && !votedFor.equals(candidateId) 
						|| lastLogTerm < myLastLogTerm) 
						|| (lastLogTerm == myLastLogTerm && lastLogIndex < myLogEnd)) {
					peer.queueMessage("VOTE_RESPONSE " + elecState.getCurrentTerm() + " NO " + nodeId);
					return;
				} 

				elecState.voteFor(candidateId);
				operationProcessor.persistElecState(elecState.getElectionState());
				electionDeadline = System.nanoTime() + 
						TimeUnit.MILLISECONDS.toNanos(rand.nextInt(800, 1501));

				peer.queueMessage("VOTE_RESPONSE " + elecState.getCurrentTerm() + " YES " + nodeId);
			}

		} else if (msg.startsWith("VOTE_RESPONSE ")) {
			synchronized (electionLock) {
				long voterTerm = Long.parseLong(parts[1]);
				String answer = parts[2];
				String voterId = parts[3];

				long myTerm = elecState.getCurrentTerm();


				if (voterTerm > myTerm) {
					stepDown();
					elecState.observeHigherTerm(voterTerm);
					operationProcessor.persistElecState(elecState.getElectionState());
					return;

				} else if (answer.equals("NO") || voterTerm < myTerm) {
					return;

				} else if (voterTerm == myTerm && role == NodeRole.CANDIDATE) {
					myVotes.add(voterId);

					if (myVotes.size() >= 2) {
						becomeLeader();
					}

				} 

			}

		} else {
			System.err.println(
					"Unknown cluster election message received: " + msg
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
		synchronized (electionLock) {
			if (role == NodeRole.LEADER) {
				return;
			}

			role = NodeRole.CANDIDATE;
			opStatus = OperationalStatus.SYNCING;

			long term = elecState.startNewTerm();
			String votedFor = elecState.voteFor(nodeId);

			myVotes = new HashSet<>();
			myVotes.add(votedFor);

			operationProcessor.persistElecState(elecState.getElectionState());

			electionDeadline = System.nanoTime() + 
					TimeUnit.MILLISECONDS.toNanos(rand.nextInt(800,1501));

			long lastLogTerm = operationProcessor.retrieveLastLogTerm();
			long lastLogIndex = repState.getLogEnd();

			for (PeerConnection peer : peers.values()) {
				if ( peer != null) {
					peer.queueMessage("REQUEST_VOTE " + term + " " + lastLogTerm + " " + lastLogIndex + " " + nodeId);
				}
			}

			leaderId = null;

		}
	}

	public void becomeLeader() throws IOException {
		role = NodeRole.LEADER;
		opStatus = OperationalStatus.SYNCING;
		leaderId = nodeId;
		sendHeartbeat();

		Long entryTerm = elecState.getCurrentTerm();
		Long index = operationProcessor.persistOperation(entryTerm, new String[]{"NO_OP"});

		long prevEntryTerm = index == 1
				? 0L
						: operationProcessor.retrieveLogTerm(index - 1);

		sendToFollowers("REPLICATE " + entryTerm + " " + nodeId + " " + prevEntryTerm + " " + index + " " + entryTerm + " NO_OP");

	}

	public boolean acceptLeader(long leaderTerm, String leaderId) throws IOException {
		synchronized (electionLock) {
			long myTerm = elecState.getCurrentTerm();

			if (leaderTerm < myTerm) {
				return false;

			} else if (leaderTerm == myTerm) {

				if (role == NodeRole.LEADER ||
						(this.leaderId != null && !this.leaderId.equals(leaderId))) {
					return false;
				}

				if (this.leaderId == null) {
					opStatus = OperationalStatus.SYNCING;
				}

				if(role == NodeRole.CANDIDATE) {
					stepDown();
				}

			} else {
				stepDown();
				elecState.observeHigherTerm(leaderTerm);
				operationProcessor.persistElecState(elecState.getElectionState());
			}

			this.leaderId = leaderId;
			electionDeadline = System.nanoTime()
					+ TimeUnit.MILLISECONDS.toNanos(rand.nextInt(800, 1501));

			return true;
		}
	}

	public void stepDown() {
		synchronized (electionLock) {
			NodeRole oldRole = role;
			role = NodeRole.FOLLOWER;
			opStatus = OperationalStatus.SYNCING;

			if (oldRole == NodeRole.LEADER) {
				pendingForwardedRequests.clear();
			}

			leaderId = null;

		}
	}
}
