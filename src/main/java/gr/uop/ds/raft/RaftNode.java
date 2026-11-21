package gr.uop.ds.raft;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static gr.uop.ds.raft.NodeLogger.Kind.APPLY;
import static gr.uop.ds.raft.NodeLogger.Kind.CLIENT;
import static gr.uop.ds.raft.NodeLogger.Kind.COMMIT;
import static gr.uop.ds.raft.NodeLogger.Kind.DISK;
import static gr.uop.ds.raft.NodeLogger.Kind.ELECT;
import static gr.uop.ds.raft.NodeLogger.Kind.ERROR;
import static gr.uop.ds.raft.NodeLogger.Kind.PEER;
import static gr.uop.ds.raft.NodeLogger.Kind.RECV;
import static gr.uop.ds.raft.NodeLogger.Kind.REPL;
import static gr.uop.ds.raft.NodeLogger.Kind.SEND;
import static gr.uop.ds.raft.NodeLogger.Kind.STATE;
import static gr.uop.ds.raft.NodeLogger.Kind.VOTE;
import gr.uop.ds.raft.proto.AppendRequest;
import gr.uop.ds.raft.proto.AppendResponse;
import gr.uop.ds.raft.proto.CommandRequest;
import gr.uop.ds.raft.proto.CommandResponse;
import gr.uop.ds.raft.proto.LogEntry;
import gr.uop.ds.raft.proto.RaftServiceGrpc;
import gr.uop.ds.raft.proto.VoteRequest;
import gr.uop.ds.raft.proto.VoteResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

/**
 * One server of the Raft cluster.
 * It holds the Raft state (term, vote, log, commit index), answers the two RPCs
 * (RequestVote, AppendEntries) through RaftServiceImpl, runs the election timer
 * and, while it is the leader, replicates its log to the followers with periodic heartbeats.
 * Committed commands are applied, in log order, to the KvStore.
 *
 * Thread safety: all Raft state is guarded by the lock of this object (synchronized).
 * Network calls are always made OUTSIDE the lock, so a slow peer never freezes the node.
 */
public class RaftNode {

    /** The three roles of a Raft server. Every node starts as a FOLLOWER. */
    enum Role { FOLLOWER, CANDIDATE, LEADER }

    // we change this values ONLY in a synchronized block or method

    // Persistent state: saved to disk before we answer
    private long currentTerm = 0;
    private String votedFor = null;
    private final RaftLog log;

    // Volatile state: lost on a crash and rebuilt afterwards
    private long commitIndex = 0;   // highest index known to be committed
    private long lastApplied = 0;   // highest index applied to the state machine
    private Role role = Role.FOLLOWER;
    private String currentLeader = null; // who is the leader or null if we don't have one
    private final KvStore kv = new KvStore(); // the state machine; rebuilt by re-applying the log after a restart

    // Leader-only volatile state: reinitialized in becomeLeader() after every election
    private final Map<String, Long> nextIndex = new HashMap<>();   // next entry to send to each peer
    private final Map<String, Long> matchIndex = new HashMap<>();  // highest entry known to be on each peer
    private final Map<Long, CompletableFuture<String>> pending = new HashMap<>(); // index -> waiting client
    private final Map<String, Long> lastAckAt = new HashMap<>(); // when each peer last answered us

    // Infrastructure: not part of the Raft algorithm, just what we need to run it
    private final String nodeId;
    private final PersistentState storage; // writes currentTerm and votedFor to data/<nodeId>/state.txt
    private final Map<String, RaftServiceGrpc.RaftServiceBlockingStub> peers = new LinkedHashMap<>(); // one gRPC stub per other node
    private final Map<String, ManagedChannel> channels = new LinkedHashMap<>(); // the connection behind each stub
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2); // runs the election timer and the heartbeats
    private ScheduledFuture<?> electionTimer; // the pending startElection(); resetElectionTimer() replaces it

    // Logging only: not used by the algorithm
    private final NodeLogger out;
    private final Stats stats = new Stats();   // counters for the experiments (STATS command)
    private volatile boolean preVoteEnabled = Boolean.parseBoolean(System.getProperty("raft.prevote", "true"));   // PREVOTE on|off, or -Draft.prevote=false
    private final boolean checkQuorumEnabled = Boolean.parseBoolean(System.getProperty("raft.checkQuorum", "true")); // -Draft.checkQuorum=false
    private final Map<String, Boolean> peerUp = new HashMap<>(); // last known reachability, so we print only changes
    private long electionTimeoutMs;
    private long lastHeartbeatAt = System.currentTimeMillis();
    private final Map<String, RaftServiceGrpc.RaftServiceStub> asyncPeers = new LinkedHashMap<>(); // non-blocking stubs: the answer arrives later in a callback
    private final Set<String> votesGranted = new HashSet<>(); // who voted for us in the current election (only while CANDIDATE)
    private final Set<String> inFlight = new HashSet<>();   // followers with an AppendEntries on the way (at most one each)
    private int votesAnswered;                                 // how many peers answered or failed in the current election
    private long preVoteTerm = 0;                                 // the term our running pre-vote asks for (0 = no pre-vote running)
    private final Set<String> preVotesGranted = new HashSet<>(); // who said "yes" in the running pre-vote
    private int preVotesAnswered;                                 // how many peers answered or failed in the running pre-vote

    // Timing in ms, configurable for the experiments: -Draft.heartbeat=100 -Draft.electionMin=1500 -Draft.electionMax=3000
    private static final long HEARTBEAT_MS    = Long.getLong("raft.heartbeat", 500);
    private static final long ELECTION_MIN_MS = Long.getLong("raft.electionMin", 1500);
    private static final long ELECTION_MAX_MS = Long.getLong("raft.electionMax", 3000);

    /**
     * Creates the node as a FOLLOWER and restores currentTerm and votedFor from disk,
     * so a restarted node never goes back to an older term and never forgets its vote.
     * Also opens one gRPC channel to every other node of the cluster.
     * @param nodeId  the id of this node, e.g. "node1"
     * @param cluster all nodes of the cluster (id -> port), including this one
     * @param faults the fault injector; every RPC we send passes through it
     * @throws IOException if the saved state cannot be read
     */
    public RaftNode(String nodeId, Map<String, Integer> cluster, FaultInjector faults) throws IOException {
        this.nodeId = nodeId;
        this.out = new NodeLogger(nodeId);
        this.storage = new PersistentState(nodeId);
        this.log = new RaftLog(new LogStorage(Path.of("data", nodeId)));
        PersistentState.State saved = storage.load();
        this.currentTerm = saved.currentTerm();
        this.votedFor = saved.votedFor();
        for (var e : cluster.entrySet()) {
            if (e.getKey().equals(nodeId)) continue;
            ManagedChannel ch = ManagedChannelBuilder.forAddress("localhost", e.getValue())
                    .usePlaintext().intercept(faults).build();
            channels.put(e.getKey(), ch);
            ch.getState(true);
            peers.put(e.getKey(), RaftServiceGrpc.newBlockingStub(ch));
            asyncPeers.put(e.getKey(), RaftServiceGrpc.newStub(ch));
        }
        print(DISK, "loaded state.txt: term=" + currentTerm + ", votedFor=" + (votedFor == null ? "none" : votedFor));
        print(DISK, "loaded log.bin: " + log.lastIndex() + " entries (last " + log.lastIndex() + "/t" + log.lastTerm() + ")");
        print(STATE, "started as FOLLOWER, peers=" + peers.keySet());
        print(STATE, "timing: heartbeat=" + HEARTBEAT_MS + " ms, election timeout=" + ELECTION_MIN_MS + "-" + ELECTION_MAX_MS
        + " ms, pre-vote=" + (preVoteEnabled ? "on" : "off") + ", check-quorum=" + (checkQuorumEnabled ? "on" : "off"));
    }

    public String getNodeId() {return this.nodeId;}

    /**
     * Starts the two background tasks: the election timer (on every node) and the
     * heartbeat task every HEARTBEAT_MS (it does nothing unless this node is the leader).
     * Called after the gRPC server is up, so we can already answer our peers.
     */
    public void start() {
        resetElectionTimer();
        scheduler.scheduleAtFixedRate(this::sendHeartbeatsIfLeader, 0, HEARTBEAT_MS, TimeUnit.MILLISECONDS);
    }

    // Election Timer is random because we don't want all of them to become a candidate and vote them self because we are going to be in a infinite election
    private synchronized void resetElectionTimer() {
        if (electionTimer != null) electionTimer.cancel(false);
        long timeout = ELECTION_MIN_MS + ThreadLocalRandom.current().nextLong(ELECTION_MAX_MS - ELECTION_MIN_MS);  // We can change the ms in the exec command
        electionTimeoutMs = timeout;
        electionTimer = scheduler.schedule(this::onElectionTimeout, timeout, TimeUnit.MILLISECONDS);
    }

    /**
     * this method is called when another node has a newer term, then i need to become a follower
     * @param term its the recent term that the node has
     * @param from the node that sent us this term
     */
    private synchronized void stepDownIfNewerTerm(long term, String from) {
        if(term > currentTerm) {
            long oldTerm = currentTerm;
            Role oldRole = role;
            currentTerm = term;
            votedFor = null; // to vote another candidate
            // persist BEFORE we answer anything in the new term
            storage.save(currentTerm, votedFor);
            role = Role.FOLLOWER;
            if (oldRole == Role.LEADER) stats.steppedDown();
            stats.leaderLost(System.currentTimeMillis());
            lastHeartbeatAt = System.currentTimeMillis();
            if (oldRole == Role.FOLLOWER) {
                print(STATE, "term " + oldTerm + " -> " + term + ": " + from + " has a newer term");
            } else {
                print(STATE, oldRole + " -> FOLLOWER: " + from + " has newer term " + term + " (ours was " + oldTerm + ")");
            }
            currentLeader = null;
            print(DISK, "saved state.txt: term=" + currentTerm + ", votedFor=none");
            if (oldRole == Role.LEADER) {
                // we are not the leader anymore
                for (CompletableFuture<String> f : pending.values()) {
                    f.completeExceptionally(new IllegalStateException("lost leadership"));
                }
                pending.clear();
            }
        }
    }

    /**
     * Handles a RequestVote RPC a candidate asks for our vote. We grant at most one vote per term,
     * and only to a candidate whose log is at least as up-to-date as ours.
     * @param req a request from a candidate to vote him
     * @return a responds if i want to vote for him or not
     */
    public synchronized VoteResponse onRequestVote(VoteRequest req) {
        VoteResponse rsp;
        String candidate = req.getCandidateId();
        String reason;
        print(RECV, "<- " + candidate + " " + NodeLogger.fmt(req));
        stepDownIfNewerTerm(req.getTerm(), candidate); // if i don't have the new term i need to get it and become a follower
        if(req.getTerm() < currentTerm) { // he was an old candidate, i can't vote him
            rsp = VoteResponse.newBuilder()
                    .setTerm(currentTerm)
                    .setVoteGranted(false)
                    .build();
            reason = "DENIED to " + candidate + ": its term " + req.getTerm() + " is older than ours " + currentTerm;
        }  else if ((votedFor == null || votedFor.equals(req.getCandidateId())) && log.isUpToDate(req.getLastLogTerm(), req.getLastLogIndex()))  { // i vote the candidate if his log is updated with the most recent log and if the same request came back.
            votedFor = req.getCandidateId();
            storage.save(currentTerm, votedFor); // persist the vote BEFORE we answer, so after a crash we can't vote twice in this term
            resetElectionTimer(); // give this candidate time to win instead of starting our own election
            rsp = VoteResponse.newBuilder()
                    .setTerm(currentTerm)
                    .setVoteGranted(true)
                    .build();
            reason = "GRANTED to " + candidate + " for term " + currentTerm;
        } else { // already voted or the log of the candidate is not updated
            rsp = VoteResponse.newBuilder()
                    .setTerm(currentTerm)
                    .setVoteGranted(false)
                    .build();
            if (votedFor != null && !votedFor.equals(candidate)) {
                reason = "DENIED to " + candidate + ": already voted for " + votedFor + " in term " + currentTerm;
            } else {
                reason = "DENIED to " + candidate + ": its log (last " + req.getLastLogIndex() + "/t" + req.getLastLogTerm()
                        + ") is behind ours (last " + log.lastIndex() + "/t" + log.lastTerm() + ")";
            }
        }
        print(VOTE, reason);
        if (rsp.getVoteGranted()) {
            print(DISK, "saved state.txt: term=" + currentTerm + ", votedFor=" + votedFor);
        }
        print(SEND, "-> " + candidate + " " + NodeLogger.fmt(rsp));
        return rsp;
    }

    /** A peer answered our RequestVote. Runs on a gRPC thread, so it takes the lock. */
    private synchronized void onVoteResponse(String peer, long electionTerm, VoteResponse resp) {
        markPeer(peer, true, null);
        print(RECV, "<- " + peer + " " + NodeLogger.fmt(resp));
        if (resp.getTerm() > currentTerm) {
            stepDownIfNewerTerm(resp.getTerm(), peer);   // the term of the ANSWER, even if the answer is late
            return;
        }
        if (role != Role.CANDIDATE || currentTerm != electionTerm) {
            return;   // a late answer from an old election, or we already won
        }
        votesAnswered++;
        if (resp.getVoteGranted()) {
            votesGranted.add(peer);
            int majority = (peers.size() + 1) / 2 + 1;
            if (votesGranted.size() >= majority) {
                print(ELECT, "WON term " + electionTerm + " with " + votesGranted.size() + "/" + (peers.size() + 1)
                        + " votes " + votesGranted);
                becomeLeader();
                return;
            }
        }
        checkElectionOver(electionTerm);
    }

    /** Our RequestVote to this peer failed (down, too slow, or dropped by the FaultInjector). */
    private synchronized void onVoteFailed(String peer, long electionTerm, String why) {
        markPeer(peer, false, why);
        print(ERROR, "RequestVote to " + peer + " failed: " + why);
        channels.get(peer).resetConnectBackoff();
        if (role != Role.CANDIDATE || currentTerm != electionTerm) return;   // old election
        votesAnswered++;
        checkElectionOver(electionTerm);
    }

    // must be called while holding the lock
    // prints NO MAJORITY once, when every peer has answered and we still have not won
    private void checkElectionOver(long electionTerm) {
        if(role == Role.CANDIDATE && currentTerm == electionTerm && votesAnswered == peers.size()) {
            print(ELECT, "NO MAJORITY in term " + electionTerm + ": " + votesGranted.size() + "/" + (peers.size() + 1)
            + " votes " + votesGranted + " -> new election after the next timeout");
        }  
    }

    /**
     * Pre-Vote (Ongaro's dissertation): a node asks "would you vote for me in term X?"
     * before it really starts an election. We answer without changing anything:
     * no new term, no votedFor, no timer reset.
     */
    public synchronized VoteResponse onPreVote(VoteRequest req) {
        long now = System.currentTimeMillis();
        boolean heardFromLeaderRecently =
                role == Role.LEADER || (currentLeader != null && now - lastHeartbeatAt < ELECTION_MIN_MS);
        boolean grant = req.getTerm() >= currentTerm
        && log.isUpToDate(req.getLastLogTerm(), req.getLastLogIndex())
        && !heardFromLeaderRecently;

        print(VOTE, "PRE-VOTE " + (grant ? "yes" : "no") + " to " + req.getCandidateId()
                + " for term " + req.getTerm()
                + (heardFromLeaderRecently ? " (we have a leader)" : ""));
        return VoteResponse.newBuilder()
                .setTerm(currentTerm)
                .setVoteGranted(grant)
                .build();
    }

    /**
     * Handles an AppendEntries RPC from a leader. It is both the heartbeat (no entries)
     * and the way the leader replicates its log.
     * A request from an older term is rejected, so the old leader learns our term and steps down.
     * Otherwise we accept the sender as our leader, reset our election timer, run the
     * consistency check (tryAppend) and move our commitIndex forward.
     * @param req the leader's request
     * @return our current term, and success = true only if our log matched at prevLogIndex
     */
    public synchronized AppendResponse onAppendEntries(AppendRequest req) {
        AppendResponse rsp;
        String leader = req.getLeaderId();
        boolean show = req.getEntriesCount() > 0 || out.isVerbose(); // empty heartbeats only in verbose mode
        if(req.getTerm() < currentTerm) { // Old Leader
            rsp = AppendResponse.newBuilder()
                    .setTerm(currentTerm)
                    .setSuccess(false)
                    .build();
            print(RECV, "<- " + leader + " " + NodeLogger.fmt(req));
            print(REPL, "REJECT: " + leader + " is an old leader (term " + req.getTerm() + " < ours " + currentTerm + ")");
            print(SEND, "-> " + leader + " " + NodeLogger.fmt(rsp));
        } else { // if a candidate sees a leader in the same term he needs to resign
            stepDownIfNewerTerm(req.getTerm(), leader);
            if (role != Role.FOLLOWER) {
                print(STATE, role + " -> FOLLOWER: " + leader + " is the leader of term " + req.getTerm());
            }
            role = Role.FOLLOWER;
            if (!leader.equals(currentLeader)) {
                print(STATE, "following leader " + leader + " (term " + req.getTerm() + ")");
            }
            currentLeader = req.getLeaderId();
            stats.leaderFound();
            lastHeartbeatAt = System.currentTimeMillis();
            resetElectionTimer(); // the leader is alive, so we must not start an election
            long ourPrevTerm = log.termAt(req.getPrevLogIndex());
            long conflictAt = firstConflict(req.getEntriesList());
            // consistency check: accept the entries only if our log has prevLogIndex with prevLogTerm (Log Matching)
            boolean ok = log.tryAppend(req.getPrevLogIndex(), req.getPrevLogTerm(), req.getEntriesList());
            if (!ok) show = true;
            if (show) print(RECV, "<- " + leader + " " + NodeLogger.fmt(req));
            long conflictIndex = 0;
            if (!ok && ourPrevTerm == -1) {
                conflictIndex = log.lastIndex() + 1;
                print(REPL, "REJECT: we don't have entry " + req.getPrevLogIndex() + " yet (our log ends at " + log.lastIndex() + ")" + ", hint: try " + conflictIndex);
            } else if (!ok) {
                conflictIndex = req.getPrevLogIndex();
                while (conflictIndex > 1 && log.termAt(conflictIndex - 1) == ourPrevTerm) {
                    conflictIndex--;
                }
                print(REPL, "REJECT: term mismatch at " + req.getPrevLogIndex() + " (ours t" + ourPrevTerm + ", leader's t" + req.getPrevLogTerm() + ")" + ", hint: try " + conflictIndex);
            } else if (req.getEntriesCount() > 0) {
                if (conflictAt > 0) {
                    print(REPL, "CONFLICT at " + conflictAt + ": deleted our entries from " + conflictAt + " on and took the leader's");
                }
                print(REPL, "ACCEPT " + req.getEntriesCount() + " entries after " + req.getPrevLogIndex()
                        + ", log ends at " + log.lastIndex() + ", saved to log.bin");
            }
            if(ok && req.getLeaderCommit() > commitIndex) {
                long oldCommit = commitIndex;
                commitIndex = Math.min(req.getLeaderCommit(), req.getPrevLogIndex() + req.getEntriesCount()); // min because the follower knows that his log are the same with the leader if only he has at least prevLogIndex + entriesCount
                if (commitIndex != oldCommit) {
                    print(COMMIT, "commitIndex " + oldCommit + " -> " + commitIndex + " (leaderCommit=" + req.getLeaderCommit() + ")");
                }
            }
            applyCommitted(); // apply to the KvStore everything that just became committed
            rsp = AppendResponse.newBuilder()
                    .setTerm(currentTerm)
                    .setSuccess(ok)
                    .setConflictIndex(conflictIndex)
                    .build();
            if (show) print(SEND, "-> " + leader + " " + NodeLogger.fmt(rsp));
        }
        return rsp;
    }

    // first index where the leader's entries disagree with ours (0 = none); used only for printing
    private long firstConflict(List<LogEntry> entries) {
        for (LogEntry e : entries) {
            long ours = log.termAt(e.getIndex());
            if (ours != -1 && ours != e.getTerm()) return e.getIndex();
        }
        return 0;
    }

    /**
     * Leader side of a client command: appends it to the log and waits
     * until it is committed and applied, so the client gets the real result.
     * A follower answers "not the leader".
     * @param command the client's command, e.g. "PUT x 5"
     */
    public CommandResponse submit(String command) {
        long index;
        CompletableFuture<String> future = new CompletableFuture<>();
        synchronized (this) {
            if (role != Role.LEADER) {
                print(CLIENT, "\"" + command + "\" rejected: not the leader, the leader is "
                        + (currentLeader == null ? "unknown" : currentLeader));
                return CommandResponse.newBuilder()
                                    .setSuccess(false)
                                    .setResult("not the leader")
                                    .setLeaderId(currentLeader != null ? currentLeader : "")
                                    .build();
            }
            index = log.lastIndex() + 1;
            log.append(LogEntry.newBuilder().setIndex(index).setTerm(currentTerm).setCommand(command).build());
            pending.put(index, future);
            stats.appended(index);
            print(CLIENT, "\"" + command + "\" appended at index " + index + " (term " + currentTerm
                    + "), saved to log.bin, waiting for a majority");
            
        }
        
        sendHeartbeatsIfLeader();   // replicate now instead of waiting up to 500 ms for the next tick
        
        try {
            String result = future.get(1500, TimeUnit.MILLISECONDS);
            return CommandResponse.newBuilder()
                    .setSuccess(true)
                    .setResult(result)
                    .setLeaderId(nodeId)
                    .setIndex(index)
                        .build();
        } catch (TimeoutException e) {
            synchronized (this) {
                pending.remove(index);
            }
            print(ERROR, "\"" + command + "\" (index " + index + ") was not committed within 1500 ms");
            return CommandResponse.newBuilder()
                    .setSuccess(false)
                    .setResult("timeout: not committed yet, outcome unknown (no majority?)")
                    .setLeaderId(nodeId)
                    .setIndex(index)
                    .build();
        } catch (ExecutionException | InterruptedException e) {
            String why = e.getCause() != null ? e.getCause().getMessage() : e.getMessage();
            return CommandResponse.newBuilder()
                    .setSuccess(false)
                    .setResult("unknown outcome: " + why)
                    .setLeaderId("")
                    .setIndex(index)
                    .build();
        }
                                  
    }

    // Pre-Vote: asks every peer at the same time if they WOULD vote for us in term currentTerm + 1.
    // Our term stays the same, so a node that is cut off can never raise it.
    private void startPreVote() {
        long proposedTerm;
        VoteRequest req;
        synchronized (this) {
            if (role == Role.LEADER) return;
            proposedTerm = currentTerm + 1;
            print(ELECT, "election timeout (" + electionTimeoutMs + " ms): no heartbeat from "
                    + (currentLeader == null ? "any leader" : "leader " + currentLeader) + " for "
                    + (System.currentTimeMillis() - lastHeartbeatAt) + " ms -> PRE-VOTE for term " + proposedTerm);
            preVoteTerm = proposedTerm;
            preVotesGranted.clear();
            preVotesAnswered = 0;
            preVotesGranted.add(nodeId);
            req = VoteRequest.newBuilder()
                    .setTerm(proposedTerm)
                    .setCandidateId(nodeId)
                    .setLastLogIndex(log.lastIndex())
                    .setLastLogTerm(log.lastTerm())
                    .build();
            resetElectionTimer();   // if the pre-vote fails, we try again after the next timeout
        }
        // outside the lock, like startElection
        for (String id : asyncPeers.keySet()) {
            print(SEND, "-> " + id + " PreVote " + NodeLogger.fmt(req));
            asyncPeers.get(id).withDeadlineAfter(300, TimeUnit.MILLISECONDS)
                    .preVote(req, new StreamObserver<VoteResponse>() {
                        @Override public void onNext(VoteResponse resp) { onPreVoteResponse(id, proposedTerm, resp); }
                        @Override public void onError(Throwable t) {
                            onPreVoteFailed(id, proposedTerm, Status.fromThrowable(t).getCode().toString());
                        }
                        @Override public void onCompleted() { }
                    });
        }
    }

    /**
     * CheckQuorum: a leader that has not heard from a majority for a whole
     * election timeout steps down, so the others can elect a leader that a majority can reach.
     * @return true if we are still the leader and may send heartbeats
     */
    private synchronized boolean checkQuorum() {
        if (role != Role.LEADER) return false;
        if (!checkQuorumEnabled) return true;
        long now = System.currentTimeMillis();
        int heard = 1; // ourselves
        for (String peer : peers.keySet()) {
            if (now - lastAckAt.getOrDefault(peer, 0L) < ELECTION_MIN_MS) heard++;
        }
        int majority = (peers.size() + 1) / 2 + 1;
        if (heard >= majority) return true;

        print(STATE, "LEADER -> FOLLOWER: CheckQuorum failed, only " + heard + "/" + (peers.size() + 1)
                + " nodes answered in the last " + ELECTION_MIN_MS + " ms");
        role = Role.FOLLOWER;   // same term: we did not see a newer one, we just cannot reach a majority
        currentLeader = null;
        stats.steppedDown();
        stats.leaderLost(now);
        for (CompletableFuture<String> f : pending.values()) {
            f.completeExceptionally(new IllegalStateException("lost leadership"));
        }
        pending.clear();
        resetElectionTimer();
        return false;
    }

    /** A peer answered our PreVote. Runs on a gRPC thread, so it takes the lock. */
    private synchronized void onPreVoteResponse(String peer, long proposedTerm, VoteResponse resp) {
        markPeer(peer, true, null);
        print(RECV, "<- " + peer + " PreVote answer " + NodeLogger.fmt(resp));
        if(resp.getTerm() > currentTerm) {
            stepDownIfNewerTerm(resp.getTerm(), peer);
            preVoteTerm = 0; // close the pre-vote
            return;
        } else if (preVoteTerm != proposedTerm || currentTerm != proposedTerm - 1) {
            return;   // stale answer from an old pre-vote, or our term changed meanwhile
        }
        preVotesAnswered++;
        if (resp.getVoteGranted()) {
            preVotesGranted.add(peer);
            int majority = (peers.size() + 1) / 2 + 1;
            if (preVotesGranted.size() >= majority) {
                print(ELECT, "PRE-VOTE passed for term " + proposedTerm + " with "
                        + preVotesGranted.size() + "/" + (peers.size() + 1) + " votes " + preVotesGranted);
                preVoteTerm = 0; // close the pre-vote
                scheduler.execute(this::startElection); // NOT a direct call: we hold the lock
                return;
            }
        }
        checkPreVoteOver(proposedTerm);
    }

    /** Our PreVote to this peer failed (down, too slow, or dropped by the FaultInjector). */
    private synchronized void onPreVoteFailed(String peer, long proposedTerm, String why) {
        markPeer(peer, false, why);
        print(ERROR, "PreVote to " + peer + " failed: " + why);
        channels.get(peer).resetConnectBackoff();   // always, so the next call can reconnect at once
        if (preVoteTerm != proposedTerm || currentTerm != proposedTerm - 1) return;   // old pre-vote: nothing to count
        preVotesAnswered++;
        checkPreVoteOver(proposedTerm);
    }

    // must be called while holding the lock
    // once every peer has answered and we still have no majority: the pre-vote failed
    private void checkPreVoteOver(long proposedTerm) {
        if(preVoteTerm == proposedTerm && preVotesAnswered == peers.size()) {
            print(ELECT, "PRE-VOTE failed for term " + proposedTerm + ": "
                    + preVotesGranted.size() + "/" + (peers.size() + 1) + " votes " + preVotesGranted
                    + " -> we do not raise our term");
            stats.preVoteFailed();
            preVoteTerm = 0; // close the pre-vote
        }
    }

    /**
     * The startElection method is called from the timer of this node because
     * he didn't hear the leader from the timeout and the need to elect one.
     * **/
    private void startElection() {
        long electionTerm;
        VoteRequest req;
        synchronized (this) {
            if (role == Role.LEADER) { resetElectionTimer(); return; } // I am already the leader
            if (preVoteEnabled && currentLeader != null && System.currentTimeMillis() - lastHeartbeatAt < ELECTION_MIN_MS) return;  // while we were asking for pre-votes, a leader may have contacted us: then no election
            long silence = System.currentTimeMillis() - lastHeartbeatAt;
            if (!preVoteEnabled) {
                print(ELECT, "election timeout (" + electionTimeoutMs + " ms): no heartbeat from "
                        + (currentLeader == null ? "any leader" : "leader " + currentLeader) + " for " + silence + " ms");
            }
            Role oldRole = role;
            currentTerm++;
            role = Role.CANDIDATE;
            votedFor = nodeId;
            stats.electionStarted();
            if (currentLeader != null) stats.leaderLost(lastHeartbeatAt);   // without leader since its last heartbeat
            currentLeader = null;
            storage.save(currentTerm, votedFor); // writing in the disk
            votesGranted.clear();
            votesGranted.add(nodeId);
            votesAnswered = 0;
            req = VoteRequest.newBuilder()
                    .setTerm(currentTerm)
                    .setCandidateId(nodeId)
                    .setLastLogIndex(log.lastIndex())
                    .setLastLogTerm(log.lastTerm())
                    .build();
            electionTerm = currentTerm;
            print(STATE, oldRole + " -> CANDIDATE: starting election for term " + electionTerm + ", voted for myself");
            print(DISK, "saved state.txt: term=" + currentTerm + ", votedFor=" + nodeId);
            resetElectionTimer();   //if the Voting ends with a tie then we try again later
        }
        
        // outside the lock: ask every peer at the same time, nobody waits for anybody
        for (String id : asyncPeers.keySet()) {
            print(SEND, "-> " + id + " " + NodeLogger.fmt(req));
            asyncPeers.get(id).withDeadlineAfter(300, TimeUnit.MILLISECONDS)
                    .requestVote(req, new StreamObserver<VoteResponse>() {
                        @Override public void onNext(VoteResponse resp) { onVoteResponse(id, electionTerm, resp); }
                        @Override public void onError(Throwable t) {
                            onVoteFailed(id, electionTerm, Status.fromThrowable(t).getCode().toString());
                        }
                        @Override public void onCompleted() { }
                    });
        }
    }

    // the election timer fired: with Pre-Vote we first ask, otherwise we start the election at once
    private void onElectionTimeout() {
        if (preVoteEnabled) startPreVote();
        else startElection();
    }

    // Heartbeats (only the leader)
    /**
     * this method it is called every HEARTBEAT_MS to sent data to the followers or tells them that is still the leader and it is called
     * on the submit method for every new command that the client sends
     */
    private void sendHeartbeatsIfLeader() {
        if (!checkQuorum()) return;
        for (String peer : peers.keySet()) replicateTo(peer); // never waits, at most one request in flight per follower
    }

    /**
     * Handles a follower's reply to an AppendEntries we sent.
     * On success, records how far the follower's log matches ours and tries to advance the commit index.
     * On failure, moves nextIndex back to the follower's hint (or one step back if there is none) 
     * and never below matchIndex + 1 so the next round probes an earlier position.
     * @param peer id of the follower that replied
     * @param req  the request WE sent (tells us exactly which entries the follower confirmed)
     * @param resp the follower's reply
     */
    private synchronized void onAppendResponse(String peer, AppendRequest req, AppendResponse resp) {
        if (req.getEntriesCount() > 0 || !resp.getSuccess() || out.isVerbose()) {
            print(RECV, "<- " + peer + " " + NodeLogger.fmt(resp));
        }
        if(resp.getTerm() > currentTerm) { // has a newer term step me down
            stepDownIfNewerTerm(resp.getTerm(), peer);
        } else if(role != Role.LEADER || currentTerm != req.getTerm()) {
            // its a response from an old term or we are not the leader
            if (out.isVerbose()) print(REPL, "ignored a stale answer from " + peer + " (our request was for term " + req.getTerm() + ")");
        } else if(resp.getSuccess()) { // if he got it save it and prepare for commit
           long confirmed = req.getPrevLogIndex() + req.getEntriesCount();
            if (confirmed > matchIndex.get(peer)) {   // an older answer may arrive after a newer one and the matchIndex never moves back
                matchIndex.put(peer, confirmed);
            }
            nextIndex.put(peer, matchIndex.get(peer) + 1);
            if (req.getEntriesCount() > 0) {
                print(REPL, peer + " has our log up to " + matchIndex.get(peer)
                        + " (matchIndex=" + matchIndex.get(peer) + ", nextIndex=" + nextIndex.get(peer) + ")");
            }
            advanceCommitIndex();

        } else {
            long oldNext = nextIndex.get(peer);
            // step back to the prevLogIndex of this request, but never below what the follower already confirmed
            long target = Math.min(oldNext, req.getPrevLogIndex());
            long hint = resp.getConflictIndex();
            if (hint > 0) {
                target = Math.min(target, hint); // the follower told us where to try
            }
            nextIndex.put(peer, Math.max(matchIndex.get(peer) + 1, target));
            if (nextIndex.get(peer) != oldNext) {
                print(REPL, peer + " rejected prev=" + req.getPrevLogIndex() + "/t" + req.getPrevLogTerm()
                    + ": nextIndex " + oldNext + " -> " + nextIndex.get(peer) + " (follower's hint " + hint + ")");
            }
        }
    }

    // Reads from THIS node's copy of the store. On a follower the value may be slightly stale.
    public synchronized String localGet(String key) {
        return kv.get(key);
    }

    /**
     * Applies to the KvStore, in log order, every entry after lastApplied up to commitIndex.
     * Every node applies the same commands in the same order, so all KvStores end up identical.
     */
    // must be called while holding the lock (from a synchronized method)
    private void applyCommitted() {
        while(lastApplied < commitIndex) {
            lastApplied++;
            String command = log.get(lastApplied).getCommand();
            String result = kv.apply(command);
            print(result.startsWith("ERR") ? ERROR : APPLY, "apply " + lastApplied + ": \"" + command + "\" -> " + result);
            stats.applied(lastApplied);
            CompletableFuture<String> waiting = pending.remove(lastApplied);
            if (waiting != null) {
                waiting.complete(result);   // wake up the client that waits for this index
            }
        }
    }

    /**
     * Called once we won the election. We don't know yet how far each follower's log goes,
     * so we start optimistic (nextIndex = our last index + 1) and walk back on every failure.
     */
    // must be called while holding the lock
    private void becomeLeader() {
        inFlight.clear();
        scheduler.execute(this::sendHeartbeatsIfLeader);   // tell the followers now, not at the next HEARTBEAT_MS tick
        stats.electionWon();
        stats.leaderFound();
        Role oldRole = role;
        role = Role.LEADER;
        currentLeader = nodeId;
        for (String peer : peers.keySet()) {
            nextIndex.put(peer, log.lastIndex() + 1); // assume they have everything we have
            matchIndex.put(peer, 0L); // we have confirmed nothing yet
            lastAckAt.put(peer, System.currentTimeMillis()); // a new leader gets one full election timeout of grace
        }
        print(STATE, oldRole + " -> LEADER of term " + currentTerm + ", nextIndex=" + (log.lastIndex() + 1) + " for every peer");
        // An entry of our own term lets us commit older entries
        log.append(LogEntry.newBuilder()
                .setIndex(log.lastIndex() + 1)
                .setTerm(currentTerm)
                .setCommand("NOOP")
                .build());
        print(REPL, "appended NOOP at " + log.lastIndex() + " (term " + currentTerm + ") so that older entries can be committed");
    }

    // must be called while holding the lock
    /* this method checks if the majority has the command that the leader sent to them
    commits the highest entry that a majority has AND that belongs to my current term */
    private void advanceCommitIndex() {
        int majority = (peers.size() + 1) / 2 + 1;
        for (long n = log.lastIndex(); n > commitIndex; n--) {
            long count = 1;                          // the leader always has its own entries
            for (long m : matchIndex.values()) {
                if (m >= n) count++;                 // this follower has entry n
            }
            // only an entry of OUR term may be committed by counting (Figure 8);
            // the older entries before it become committed together with it
            if (count >= majority && log.termAt(n) == currentTerm) {
                 long oldCommit = commitIndex;
                 commitIndex = n;
                 print(COMMIT, "index " + n + " is on a majority " + holders(n) + ": commitIndex " + oldCommit + " -> " + n);
                 applyCommitted();
                 return;
            }
            if (count >= majority && out.isVerbose()) {
                print(COMMIT, "index " + n + " is on a majority but has term " + log.termAt(n)
                        + ", not ours (" + currentTerm + "): we wait for an entry of our term (Figure 8)");
            }
        }
    }

    // the nodes that have entry n (we know it from matchIndex); used only for printing
    private List<String> holders(long n) {
        List<String> result = new ArrayList<>(List.of(nodeId));
        for (var e : matchIndex.entrySet()) {
            if (e.getValue() >= n) result.add(e.getKey());
        }
        return result;
    }

    // prints one event together with our current term and role
    private synchronized void print(NodeLogger.Kind kind, String message) {
        out.log(kind, currentTerm, role, message);
    }

    // remembers whether a peer answers and prints only when this changes, not on every failed heartbeat
    private synchronized void markPeer(String peer, boolean up, String why) {
        Boolean before = peerUp.put(peer, up);
        if (before == null || before != up) {
            if (up) {
                print(PEER, peer + " is reachable");
            } else {
                print(PEER, peer + " is UNREACHABLE (" + why + ")");
            }
        }
    }

    public void setVerbose(boolean verbose) {
        out.setVerbose(verbose);
    }

    public void setPreVote(boolean enabled) {
        preVoteEnabled = enabled;
    }

    /** The STATUS command: a small table with the whole state of this node. */
    public synchronized String status() {
        StringBuilder sb = new StringBuilder();
        sb.append("------------------- ").append(nodeId).append(" -------------------\n");
        sb.append(String.format(" role     : %-10s term     : %d%n", role, currentTerm));
        sb.append(String.format(" leader   : %-10s votedFor : %s%n",
                currentLeader == null ? "unknown" : currentLeader, votedFor == null ? "none" : votedFor));
        sb.append(String.format(" log      : %d entries (last %d/t%d)%n", log.lastIndex(), log.lastIndex(), log.lastTerm()));
        sb.append(String.format(" commit   : %-10d applied  : %d%n", commitIndex, lastApplied));
        sb.append(String.format(" kv store : %d keys%n", kv.size()));
        sb.append(" peers    :\n");
        for (String p : peers.keySet()) {
            Boolean up = peerUp.get(p);
            String reach = up == null ? "unknown" : (up ? "up" : "DOWN");
            if (role == Role.LEADER) {
                sb.append(String.format("   %-7s %-8s matchIndex=%-4d nextIndex=%d%n", p, reach, matchIndex.get(p), nextIndex.get(p)));
            } else {
                sb.append(String.format("   %-7s %s (last time we talked to it)%n", p, reach));
            }
        }
        sb.append("------------------------------------------------");
        return sb.toString();
    }

    /** The LOG command: every entry of our log and whether it is committed and applied. */
    public synchronized String logView() {
        if (log.lastIndex() == 0) return " (empty log)";
        StringBuilder sb = new StringBuilder(" index  term  status     command\n");
        for (long i = 1; i <= log.lastIndex(); i++) {
            LogEntry e = log.get(i);
            String status = i <= lastApplied ? "applied" : (i <= commitIndex ? "committed" : "pending");
            sb.append(String.format(" %5d  %4d  %-9s  %s%n", i, e.getTerm(), status, e.getCommand()));
        }
        return sb.toString().stripTrailing();
    }

    /**
     * Used by the terminal: if we are the leader we run the command here,
     * otherwise we forward it to the leader with the ClientCommand RPC.
     */
    public CommandResponse execute(String command) {
        String leader;
        boolean amLeader;
        synchronized (this) {
            amLeader = (role == Role.LEADER);
            leader = currentLeader;
        }
        if (amLeader) return submit(command);  // submit waits

        if (leader == null) {
            print(CLIENT,"\"" + command + "\" rejected: no leader known yet, try again");
            return CommandResponse.newBuilder()
                                  .setResult("no leader known yet, try again")
                                  .setLeaderId("")
                                  .build();
        }

        // here we do NOT hold the lock: the network call may take long
        try {
            CommandRequest req = CommandRequest.newBuilder().setCommand(command).build();
            print(SEND, "-> " + leader + " " + NodeLogger.fmt(req));
            CommandResponse resp = peers.get(leader).withDeadlineAfter(2, TimeUnit.SECONDS).clientCommand(req);
            print(RECV, "<- " + leader + " " + NodeLogger.fmt(resp));
            return resp;
        } catch (StatusRuntimeException e) {
            print(ERROR, "ClientCommand to " + leader + " failed: " + e.getStatus().getCode());
            if (e.getStatus().getCode() == Status.Code.UNAVAILABLE) {
                // we could not reach the leader
                return CommandResponse.newBuilder()
                        .setResult("leader " + leader + " unreachable, try again")
                        .setLeaderId(leader)
                        .build();
            }
            return CommandResponse.newBuilder() // deadline
                    .setResult("unknown outcome: the command may or may not have been applied")
                    .setLeaderId(leader)
                    .build();
        }
    }

    /** Sends the next AppendEntries to one follower, unless one is already on the way. Never waits. */
    private void replicateTo(String peer) {
        AppendRequest req;
        boolean show;
        synchronized (this) {
            if (role != Role.LEADER || inFlight.contains(peer)) return;   // one request at a time per follower
            long next = nextIndex.get(peer);
            long prevIndex = next - 1;
            req = AppendRequest.newBuilder()
                    .setTerm(currentTerm)
                    .setLeaderId(nodeId)
                    .setPrevLogIndex(prevIndex)
                    .setPrevLogTerm(log.termAt(prevIndex))
                    .addAllEntries(log.entriesFrom(next))
                    .setLeaderCommit(commitIndex)
                    .build();
            show = out.isVerbose() || (req.getEntriesCount() > 0 && !Boolean.FALSE.equals(peerUp.get(peer)));
            inFlight.add(peer);
        }
        if (show) print(SEND, "-> " + peer + " " + NodeLogger.fmt(req));
        asyncPeers.get(peer).withDeadlineAfter(300, TimeUnit.MILLISECONDS)
                .appendEntries(req, new StreamObserver<AppendResponse>() {
                    @Override public void onNext(AppendResponse resp) {
                        ackFrom(peer);
                        markPeer(peer, true, null);
                        onAppendResponse(peer, req, resp);
                    }
                    @Override public void onError(Throwable t) {
                        markPeer(peer, false, Status.fromThrowable(t).getCode().toString());
                        channels.get(peer).resetConnectBackoff();
                        release(peer);                                 // if it failed wait for the next tick, no hot loop
                    }
                    @Override public void onCompleted() {
                        if (release(peer)) replicateTo(peer);          // did it answered if it still misses entries, send at once
                    }
                });
    }

    // must not be called while holding the lock from outside
    // frees the follower; returns true if we are still the leader and the follower still misses entries
    private synchronized boolean release(String peer) {
        inFlight.remove(peer); 
        return role == Role.LEADER && nextIndex.get(peer) <= log.lastIndex();
    }

    // any answer from a peer in our term proves that the peer still hears us
    private synchronized void ackFrom(String peer) {
        lastAckAt.put(peer, System.currentTimeMillis());
    }

    public synchronized String statsReport() {
        return stats.report(nodeId, currentTerm);
    }

    public synchronized void resetStats() {
        stats.reset(currentLeader != null);
    }
}