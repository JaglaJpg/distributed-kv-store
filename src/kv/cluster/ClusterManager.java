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
	private Map<String, FollowerProgress> followerProgress = new ConcurrentHashMap<>();

	private AtomicLong nextRequestId = new AtomicLong(0);

	private final OperationProcessor operationProcessor;
	private final ReplicationState repState;
	private final ElectionState elecState;

	private final Object electionLock = new Object();

	private final Random rand = new Random();
	private volatile long electionDeadline = System.nanoTime() + 
			TimeUnit.MILLISECONDS.toNanos(rand.nextInt(800, 1501));

	private Set<String> myVotes = new HashSet<>();

	public ClusterManager(
			String nodeId,
			OperationProcessor operationProcessor,
			ReplicationState repState,
			ElectionState elecState) {

		this.nodeId = nodeId;
		this.role = NodeRole.FOLLOWER;
		this.operationProcessor = operationProcessor;
		this.repState = repState;
		this.opStatus = OperationalStatus.SYNCING;
		this.elecState = elecState;
	}

	public void sendHello(PeerConnection peer) {
		peer.queueMessage("HELLO " + nodeId);
	}

	public void sendHeartbeat() throws IOException {
		synchronized(electionLock) {
			if (role != NodeRole.LEADER) {
				return;
			}
			
			for(String followerId : followerProgress.keySet()) {
				sendAppendEntries(followerId);
			}
		}
		
	}

	public void handleMessage(String msg, PeerConnection peer) throws IOException, InterruptedException {
		if (msg.startsWith("HELLO ")) {
			handleHello(msg, peer);
			return;
			
		}else if (msg.startsWith("REQUEST_VOTE ")) {
			handleRequestVote(msg, peer);
			return;
			
		} else if (msg.startsWith("VOTE_RESPONSE ")) {
			handleVoteResponse(msg, peer);
			return;
			
		}else if (msg.startsWith("APPEND_ENTRIES ")) {
			handleAppendEntries(msg, peer);
			return;
			
		} else if (msg.startsWith("APPEND_ENTRIES_RESPONSE ")) {
			handleAppendEntriesResponse(msg, peer);
			return;
			
		}else if (msg.startsWith("RESULT ")) {
			handleResult(msg, peer);
			return;
			
		} else if (msg.startsWith("FORWARD ")) {
			handleForward(msg, peer);
			return;
			
		} else {
			System.err.println(
					"Unknown cluster election message received: " + msg
					);
		}

	}

	public Response handleClientMutation(String[] tokens)
	        throws InterruptedException, ExecutionException, IOException {

	    long requestId;
	    CompletableFuture<Response> future = new CompletableFuture<>();

	    synchronized (electionLock) {

	        if (opStatus == OperationalStatus.SYNCING) {
	            return new Response(
	                    ExecutionStatus.NOT_READY,
	                    "This Node is synchronising, retry shortly"
	            );
	        }

	        requestId = nextRequestId.getAndIncrement();
	        pendingRequests.put(requestId, future);

	        if (role != NodeRole.LEADER) {

	            String message = "FORWARD " + requestId + " "
	                    + String.join(" ", tokens);

	            sendToLeader(message);

	        } else {

	            long logIndex;

	            try {
	                long entryTerm = elecState.getCurrentTerm();
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
	                    new ForwardedRequest(requestId, null)
	            );

	            try {
	                sendHeartbeat();
	            } catch (IOException e) {
	                e.printStackTrace();
	            }
	        }
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
	
	public void sendAppendEntries(String followerId) throws IOException {

	    FollowerProgress progress = followerProgress.get(followerId);

	    long term = elecState.getCurrentTerm();
	    long nextIndex = progress.nextIndex;
	    long prevLogIndex = nextIndex - 1;

	    long prevLogTerm;

	    if (prevLogIndex == 0L) {
	        prevLogTerm = 0L;

	    } else if (prevLogIndex == operationProcessor.retrieveBoundaryIndexAndTerm()[0]) {
	        prevLogTerm = operationProcessor.retrieveBoundaryIndexAndTerm()[1];
	        // TODO: Snapshot boundary term support is not yet working correctly

	    } else {
	        prevLogTerm = operationProcessor.retrieveLogTerm(prevLogIndex);
	    }

	    long leaderCommit = repState.getCommitIndex();

	    int entryCount = 0;
	    String entryPayload = "";

	    if (nextIndex <= repState.getLogEnd()) {

	        String[] entry = operationProcessor.retrieveEntry(nextIndex);

	        entryCount = 1;
	        entryPayload = " " + String.join(" ", entry);
	    }

	    PeerConnection peer = peers.get(followerId);

	    if (peer == null) {
	        return;
	    }

	    peer.queueMessage(
	            "APPEND_ENTRIES " + term + " "
	            + nodeId + " "
	            + prevLogIndex + " "
	            + prevLogTerm + " "
	            + leaderCommit + " "
	            + entryCount
	            + entryPayload
	    );
	}

	public void handleAppendEntries(String msg, PeerConnection peer) throws IOException, InterruptedException {
		String[] parts = msg.split("\\s+", 8);
		
		long term = Long.parseLong(parts[1]);
		String leaderId = parts[2];
		long prevLogIndex = Long.parseLong(parts[3]);
		long prevLogTerm = Long.parseLong(parts[4]);
		long leaderCommit = Long.parseLong(parts[5]);
		int entryCount = Integer.parseInt(parts[6]);
		
		synchronized (electionLock) {
			if (!acceptLeader(term, leaderId)) {
				peer.queueMessage("APPEND_ENTRIES_RESPONSE "
			            + elecState.getCurrentTerm() + " false " + prevLogIndex);
				return;
			}
			
			if (prevLogIndex > repState.getLogEnd()) {
			    peer.queueMessage("APPEND_ENTRIES_RESPONSE "
			            + elecState.getCurrentTerm() + " false " + prevLogIndex);
			    return;
			}
			
			try {
				Long myPrevLogTerm;

				if (prevLogIndex == 0L) {
					myPrevLogTerm = 0L;

				} else if (prevLogIndex == operationProcessor.retrieveBoundaryIndexAndTerm()[0]) {
					myPrevLogTerm = operationProcessor.retrieveBoundaryIndexAndTerm()[1];
					// TODO: Snapshot boundary term support

				} else {
					myPrevLogTerm = operationProcessor.retrieveLogTerm(prevLogIndex);
				}

				if (myPrevLogTerm != prevLogTerm) {
					peer.queueMessage("APPEND_ENTRIES_RESPONSE " + elecState.getCurrentTerm() + " false " + prevLogIndex);
					return;
				}
				
			} catch (IllegalStateException e) {
				peer.queueMessage("APPEND_ENTRIES_RESPONSE " + elecState.getCurrentTerm() + " false " + prevLogIndex);
				return;
			}
			
			
			// Process incoming log entry (if present)
			if (entryCount == 1) {

			    // WAL entry format: index term operation...
			    String[] entry = parts[7].split("\\s+", 3);

			    long logIndex = Long.parseLong(entry[0]);
			    long entryTerm = Long.parseLong(entry[1]);
			    String[] tokens = entry[2].split("\\s+");

			    // The entry must immediately follow prevLogIndex
			    if (logIndex != prevLogIndex + 1) {
			        peer.queueMessage("APPEND_ENTRIES_RESPONSE "
			                + elecState.getCurrentTerm() + " false " + prevLogIndex);
			        return;
			    }

			    long logEnd = repState.getLogEnd();

			    // CASE 3: Current entry missing
			    if (logIndex > logEnd) {

			        operationProcessor.persistOperation(entryTerm, tokens);

			    } else {

			        // CASE 4: Current entry already exists
			        long myEntryTerm = operationProcessor.retrieveLogTerm(logIndex);

			        if (myEntryTerm == entryTerm) {

			            // CASE 4A: Same entry, nothing to change

			        } else {
			            // CASE 4B: Conflicting entry
			            try {
			                operationProcessor.truncateUncommittedFrom(logIndex);
			                operationProcessor.persistOperation(entryTerm, tokens);
			            } catch (IllegalStateException e) {
			                peer.queueMessage("APPEND_ENTRIES_RESPONSE "
			                        + elecState.getCurrentTerm() + " false " + prevLogIndex);
			                return;
			            }
			        } 
			    }
			}
			
			// Advance follower commit index
			long confirmedIndex = prevLogIndex + entryCount;

			long newCommitIndex = Math.min(leaderCommit, confirmedIndex);

			if (newCommitIndex > repState.getCommitIndex()) {
			    repState.advanceCommitIndex(newCommitIndex);
			    operationProcessor.applyCommittedThrough();
			}
			
			if (confirmedIndex >= leaderCommit && repState.getLastApplied() >= leaderCommit) {
			    opStatus = OperationalStatus.READY;
			}

			// Acknowledge successful AppendEntries
			peer.queueMessage("APPEND_ENTRIES_RESPONSE "
			        + elecState.getCurrentTerm() + " true " + confirmedIndex);
		}
		
	}
	
	public void handleAppendEntriesResponse(String msg, PeerConnection peer) throws IOException, InterruptedException {
		String[] parts = msg.split("\\s+");
		
		long term = Long.parseLong(parts[1]);
		boolean success = Boolean.parseBoolean(parts[2]);
		long index = Long.parseLong(parts[3]);
		
		synchronized (electionLock) {
			if (term > elecState.getCurrentTerm()) {
				stepDown();
				elecState.observeHigherTerm(term);
				operationProcessor.persistElecState(elecState.getElectionState());
				return;
				
			} else if (term < elecState.getCurrentTerm() || role != NodeRole.LEADER) {
				return;
				
			} 
			
			
			FollowerProgress follower = followerProgress.get(peer.getPeerId());
			
			if (follower == null) {
				return;
			}
			
			if (success) {

			    if (index < 0 || index > repState.getLogEnd()) {
			        return;
			    }

			    follower.matchIndex = Math.max(follower.matchIndex, index);
			    follower.nextIndex = follower.matchIndex + 1;

			    long oldCommitIndex = repState.getCommitIndex();

			    // Advance commit index
			    if (index > oldCommitIndex) {
			        if (operationProcessor.retrieveLogTerm(index)
			                == elecState.getCurrentTerm()) {

			            repState.advanceCommitIndex(index);
			        }
			    }

			    long newCommitIndex = repState.getCommitIndex();

			    // Apply newly committed operations
			    if (newCommitIndex > oldCommitIndex) {

			        List<Response> responses =
			                operationProcessor.applyCommittedThrough();
			        opStatus = OperationalStatus.READY;

			        int responseIndex = 0;

			        for (long logIndex = oldCommitIndex + 1;
			                logIndex <= newCommitIndex;
			                logIndex++) {

			            Response response = responses.get(responseIndex++);

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
			                            + elecState.getCurrentTerm() + " "
			                            + nodeId + " "
			                            + request.requestId() + " "
			                            + response.getStatus().name() + " "
			                            + (payload == null ? "" : payload);

			                    request.originPeer().queueMessage(message);
			                }
			            }
			        }

			        // Propagate the new commit index to followers
			        for (String followerId : followerProgress.keySet()) {
			            sendAppendEntries(followerId);
			        }

			    } else if (follower.nextIndex <= repState.getLogEnd()) {

			        // Continue replicating missing entries
			        sendAppendEntries(peer.getPeerId());
			    }

			} else if (index == follower.nextIndex - 1) {

			    long oldNextIndex = follower.nextIndex;

			    follower.nextIndex = Math.max(
			            follower.nextIndex - 1,
			            follower.matchIndex + 1
			    );

			    if (follower.nextIndex < oldNextIndex) {
			        sendAppendEntries(peer.getPeerId());
			    }
			}
			
			
			
			
		}
		
		
		
	}
	
	private void handleHello(String msg, PeerConnection peer) {
		String[] parts = msg.split("\\s+");

		String peerId = parts[1];

		peers.put(peerId, peer);
	}

	public void handleForward(String msg, PeerConnection peer) throws IOException, InterruptedException {
		 synchronized (electionLock) {

		        if (role != NodeRole.LEADER) {
		            return;
		        }

		        String[] parts = msg.split(" ", 3);

		        long requestId = Long.parseLong(parts[1]);
		        String[] tokens = parts[2].split("\\s+");

		        long entryTerm = elecState.getCurrentTerm();
		        long logIndex;

		        try {
		            logIndex = operationProcessor.persistOperation(
		                    entryTerm, tokens
		            );

		        } catch (IOException e) {
		            peer.queueMessage(
		                    "RESULT " + entryTerm + " " + nodeId + " "
		                    + requestId
		                    + " ERR_STORAGE Write failed due to a storage error."
		            );
		            return;
		        }

		        pendingForwardedRequests.put(
		                logIndex,
		                new ForwardedRequest(requestId, peer)
		        );

		        try {
		            sendHeartbeat();
		        } catch (IOException e) {
		            e.printStackTrace();
		        }
		    }
	}

	public void handleResult(String msg, PeerConnection peer) throws IOException, InterruptedException {
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
	}


	public void handleRequestVote(String msg, PeerConnection peer) throws IOException {
		synchronized (electionLock) {
			String[] parts = msg.split("\\s+");
			
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
	}
	
	public void handleVoteResponse(String msg, PeerConnection peer) throws IOException {
		synchronized (electionLock) {
			String[] parts = msg.split("\\s+");
			
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
	}
	
	public void removePeer(PeerConnection peer) {
		synchronized (electionLock) {
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
			if (role == NodeRole.LEADER || !hasTimedOut()) {
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
		
		followerProgress.clear();

		for (String follower : List.of("Melchior-1", "Balthasar-2", "Casper-3")) {
		    if (!follower.equals(nodeId)) {
		        followerProgress.put(
		            follower,
		            new FollowerProgress(repState.getLogEnd() + 1, 0)
		        );
		    }
		}

		Long entryTerm = elecState.getCurrentTerm();
		operationProcessor.persistOperation(entryTerm, new String[]{"NO_OP"});
		
		sendHeartbeat();
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
