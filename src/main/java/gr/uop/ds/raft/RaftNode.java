package gr.uop.ds.raft;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import gr.uop.ds.raft.proto.AppendRequest;
import gr.uop.ds.raft.proto.AppendResponse;
import gr.uop.ds.raft.proto.LogEntry;
import gr.uop.ds.raft.proto.RaftServiceGrpc;
import gr.uop.ds.raft.proto.VoteRequest;
import gr.uop.ds.raft.proto.VoteResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

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

    // Persistent state: implement later
    private long currentTerm = 0;
    private String votedFor = null;
    private final RaftLog log = new RaftLog();

    // Volatile state: lost on a crash and rebuilt afterwards
    private long commitIndex = 0;   // highest index known to be committed
    private long lastApplied = 0;   // highest index applied to the state machine
    private Role role = Role.FOLLOWER;
    private String currentLeader = null; // who is the leader or null if we don't have one
    private final KvStore kv = new KvStore(); // the state machine; rebuilt by re-applying the log after a restart

    // Leader-only volatile state: reinitialized in becomeLeader() after every election
    private final Map<String, Long> nextIndex = new HashMap<>();   // next entry to send to each peer 
    private final Map<String, Long> matchIndex = new HashMap<>();  // highest entry known to be on each peer

    // Infrastructure: not part of the Raft algorithm, just what we need to run it
    private final String nodeId;
    private final PersistentState storage; // writes currentTerm and votedFor to data/<nodeId>/state.txt
    private final Map<String, RaftServiceGrpc.RaftServiceBlockingStub> peers = new LinkedHashMap<>(); // one gRPC stub per other node
    private final Map<String, ManagedChannel> channels = new LinkedHashMap<>(); // the connection behind each stub
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2); // runs the election timer and the heartbeats
    private ScheduledFuture<?> electionTimer; // the pending startElection(); resetElectionTimer() replaces it

    /**
     * Creates the node as a FOLLOWER and restores currentTerm and votedFor from disk,
     * so a restarted node never goes back to an older term and never forgets its vote.
     * Also opens one gRPC channel to every other node of the cluster.
     * @param nodeId  the id of this node, e.g. "node1"
     * @param cluster all nodes of the cluster (id -> port), including this one
     * @throws IOException if the saved state cannot be read
     */
    public RaftNode(String nodeId, Map<String, Integer> cluster) throws IOException {
        this.nodeId = nodeId;
        this.storage = new PersistentState(nodeId);
        PersistentState.State saved = storage.load();
        this.currentTerm = saved.currentTerm();
        this.votedFor = saved.votedFor();
        System.out.println("LOADED TERM:" + currentTerm);
        for (var e : cluster.entrySet()) {
            if (e.getKey().equals(nodeId)) continue;
            ManagedChannel ch = ManagedChannelBuilder.forAddress("localhost", e.getValue())
                    .usePlaintext().build();
            channels.put(e.getKey(), ch);
            peers.put(e.getKey(), RaftServiceGrpc.newBlockingStub(ch));
        }
    }

    /**
     * Starts the two background tasks: the election timer (on every node) and the
     * heartbeat task every 500 ms (it does nothing unless this node is the leader).
     * Called after the gRPC server is up, so we can already answer our peers.
     */
    public void start() {
        resetElectionTimer();
        scheduler.scheduleAtFixedRate(this::sendHeartbeatsIfLeader, 0, 500, TimeUnit.MILLISECONDS);
    }

    // Election Timer is random because we don't want all of them to become a candidate and vote them self because we are going to be in a infinite election
    private synchronized void resetElectionTimer() {
        if (electionTimer != null) electionTimer.cancel(false);
        long timeout = 1500 + ThreadLocalRandom.current().nextLong(1500);  //1.5–3 s while testing
        electionTimer = scheduler.schedule(this::startElection, timeout, TimeUnit.MILLISECONDS);
    }

    /**
     * this method is called when another node has a newer term, then i need to become a follower
     * @param term its the recent term that the node has
     */
    private synchronized void stepDownIfNewerTerm(long term) {
        if(term > currentTerm) {
            currentTerm = term;
            votedFor = null; // to vote another candidate
            // persist BEFORE we answer anything in the new term
            storage.save(currentTerm, votedFor);
            role = Role.FOLLOWER;
            System.out.println("[" + this.nodeId +"] term is " + term + ": becoming follower");
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
        stepDownIfNewerTerm(req.getTerm()); // if i don't have the new term i need to get it and become a follower
        if(req.getTerm() < currentTerm) { // he was an old candidate, i can't vote him
            rsp = VoteResponse.newBuilder()
                    .setTerm(currentTerm)
                    .setVoteGranted(false)
                    .build();
        }  else if ((votedFor == null || votedFor.equals(req.getCandidateId())) && log.isUpToDate(req.getLastLogTerm(), req.getLastLogIndex()))  { // i vote the candidate if his log is updated with the most recent log and if the same request came back. 
            votedFor = req.getCandidateId();
            storage.save(currentTerm, votedFor); // persist the vote BEFORE we answer, so after a crash we can't vote twice in this term
            resetElectionTimer(); // give this candidate time to win instead of starting our own election
            rsp = VoteResponse.newBuilder()
                    .setTerm(currentTerm)
                    .setVoteGranted(true)
                    .build();

        } else { // already voted or the log of the candidate is not updated
            rsp = VoteResponse.newBuilder()
                    .setTerm(currentTerm)
                    .setVoteGranted(false)
                    .build();
        }
        return rsp;
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
        if(req.getTerm() < currentTerm) { // Old Leader
            rsp = AppendResponse.newBuilder()
                    .setTerm(currentTerm)
                    .setSuccess(false)
                    .build();
        } else { // if a candidate sees a leader in the same term he needs to resign
            stepDownIfNewerTerm(req.getTerm());
            if (role != Role.FOLLOWER) {
                System.out.println("[" + nodeId + "] term " + req.getTerm() + ": following leader " + req.getLeaderId());
            } 
            role = Role.FOLLOWER;
            currentLeader = req.getLeaderId();
            resetElectionTimer(); // the leader is alive, so we must not start an election
            // consistency check: accept the entries only if our log has prevLogIndex with prevLogTerm (Log Matching)
            boolean ok = log.tryAppend(req.getPrevLogIndex(), req.getPrevLogTerm(), req.getEntriesList());
            if(ok && req.getLeaderCommit() > commitIndex) { 
                commitIndex = Math.min(req.getLeaderCommit(), req.getPrevLogIndex() + req.getEntriesCount()); // min because the follower knows that his log are the same with the leader if only he has at least prevLogIndex + entriesCount
            }
            applyCommitted(); // apply to the KvStore everything that just became committed
            rsp = AppendResponse.newBuilder()
                    .setTerm(currentTerm)
                    .setSuccess(ok)
                    .build();

        }
        return rsp;
    }
    /**
     * Entry point for client commands.
     * A follower only tells the client who the leader is.
     * The leader appends the command to its own log; it is NOT committed yet.
     * The heartbeats replicate it, and it is applied once a majority has it.
     * @param command the client's command, e.g. "PUT x 5"
     */
    public synchronized void submit(String command) {
        if (role != Role.LEADER) {
            System.out.println("[" + nodeId + "] not the leader, try " + currentLeader);
            return;
        }
        long index = log.lastIndex() + 1;
        log.append(LogEntry.newBuilder().setIndex(index).setTerm(currentTerm).setCommand(command).build());
        System.out.println("[" + nodeId + "] appended " + index + ": " + command);
    }

    /**
     * The startElection method is called from the timer of this node because 
     * he didn't hear the leader from the timeout and the need to elect one. 
     * **/
    private void startElection() {
        long electionTerm;
        long lastLogIndex;
        long lastLogTerm;
        synchronized (this) {
            if (role == Role.LEADER) { resetElectionTimer(); return; } // I am already the leader
            currentTerm++;
            role = Role.CANDIDATE; 
            votedFor = nodeId; 
            storage.save(currentTerm, votedFor); // writing in the disk
            lastLogIndex = log.lastIndex();
            lastLogTerm  = log.lastTerm();
            electionTerm = currentTerm;
            System.out.println("[" + nodeId + "] term " + electionTerm + ": becoming candidate");
            resetElectionTimer();   //if the Voting ends with a tie then we try again later
        }

        int votes = 1;   // my vote
        for (var peer : peers.entrySet()) { // we sent to every peer the VoteRequest to vote
            try {
                VoteRequest req = VoteRequest.newBuilder()
                        .setTerm(electionTerm)
                        .setCandidateId(nodeId)
                        .setLastLogIndex(lastLogIndex)
                        .setLastLogTerm(lastLogTerm)
                        .build();
                VoteResponse resp = peer.getValue().withDeadlineAfter(300, TimeUnit.MILLISECONDS).requestVote(req);
                if(resp.getTerm() > electionTerm) { //if he has a newer term step me down
                    stepDownIfNewerTerm(resp.getTerm());
                    return;
                } else if(resp.getVoteGranted()) { // vote granted
                    votes++;
                }
            } catch (StatusRuntimeException e) { // did not make it in time
                // the peer is down or too slow, so no vote from it; resetConnectBackoff() lets us reconnect at once when it comes back
                channels.get(peer.getKey()).resetConnectBackoff();
            }
        }

        // we released the lock during the RPCs, so re-check that we are still a candidate in the same term
        synchronized (this) { 
            int majority = (peers.size() + 1) / 2 + 1;
            if(role == Role.CANDIDATE && currentTerm == electionTerm && votes >= majority) { // checking who got the majority of the votes
                becomeLeader();
                System.out.println("[" + nodeId + "] term " + electionTerm + ": is the new leader with " + votes + " votes");
            }
        }
    }

    // Heartbeats (only the leader)
    /**
     * this method it is called every 500ms to sent data to the followers or tells them that is still the leader
     */
    private void sendHeartbeatsIfLeader() {
        for (var peer : peers.entrySet()) {
            String id = peer.getKey();
            AppendRequest req;
            synchronized (this) {                       // build the request under the lock
                if (role != Role.LEADER) return; // only the leader replicates
                // we send the entries from nextIndex on; prevIndex/prevTerm describe the entry just before them,
                // the follower uses them for the consistency check
                long next = nextIndex.get(id);
                long prevIndex = next - 1; 
                long prevTerm = log.termAt(prevIndex);
                req = AppendRequest.newBuilder() // constructing the request
                            .setTerm(currentTerm)
                            .setLeaderId(nodeId)
                            .setPrevLogIndex(prevIndex)
                            .setPrevLogTerm(prevTerm)
                            .addAllEntries(log.entriesFrom(next))
                            .setLeaderCommit(commitIndex)
                            .build(); // building the request
            } try {                                       
                // send outside the lock because a network call can take long and would freeze the whole node
                AppendResponse resp = peer.getValue().withDeadlineAfter(300, TimeUnit.MILLISECONDS).appendEntries(req);
                onAppendResponse(id, req, resp);
            } catch (StatusRuntimeException e) {
                // the follower is down or slow: we simply retry on the next heartbeat (500 ms)
                channels.get(id).resetConnectBackoff();
            }
        }
    }
    
    /**
     * Handles a follower's reply to an AppendEntries we sent.
     * On success, records how far the follower's log matches ours and tries to advance the commit index.
     * On failure, moves nextIndex one step back so the next round probes an earlier position.
     * @param peer id of the follower that replied
     * @param req  the request WE sent (tells us exactly which entries the follower confirmed)
     * @param resp the follower's reply
     */
    private synchronized void onAppendResponse(String peer, AppendRequest req, AppendResponse resp) {
        if(resp.getTerm() > currentTerm) { // has a newer term step me down
            stepDownIfNewerTerm(resp.getTerm());
        } else if(role != Role.LEADER || currentTerm != req.getTerm()) {
            // its a response from an old term or we are not the leader
        } else if(resp.getSuccess()) { // if he got it save it and prepare for commit
            matchIndex.put(peer, req.getPrevLogIndex() + req.getEntriesCount());
            nextIndex.put(peer,matchIndex.get(peer) + 1);
            advanceCommitIndex();

        } else {
             nextIndex.put(peer,Math.max(1, nextIndex.get(peer) - 1)); // walk back, retry next round
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
            System.out.println("[" + nodeId + "] " + "apply " + lastApplied + ": " + log.get(lastApplied).getCommand() + "-> " + kv.apply(log.get(lastApplied).getCommand()));
        }
    }

    /**
     * Called once we won the election. We don't know yet how far each follower's log goes,
     * so we start optimistic (nextIndex = our last index + 1) and walk back on every failure.
     */
    // must be called while holding the lock
    private void becomeLeader() {
        role = Role.LEADER;
        currentLeader = nodeId;
        for (String peer : peers.keySet()) {
            nextIndex.put(peer, log.lastIndex() + 1); // assume they have everything we have
            matchIndex.put(peer, 0L); //we have confirmed nothing yet
        }
        // An entry of our own term lets us commit older entries
        log.append(LogEntry.newBuilder()
                .setIndex(log.lastIndex() + 1)
                .setTerm(currentTerm)
                .setCommand("NOOP")
                .build());
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
                 commitIndex = n; 
                 applyCommitted(); 
                 return;
            }          
        }
    }
}