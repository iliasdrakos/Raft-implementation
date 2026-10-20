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

public class RaftNode {

    enum Role { FOLLOWER, CANDIDATE, LEADER }

    private final String nodeId;
    private final Map<String, RaftServiceGrpc.RaftServiceBlockingStub> peers = new LinkedHashMap<>();
    private final Map<String, ManagedChannel> channels = new LinkedHashMap<>();
    private final Map<String, Long> nextIndex = new HashMap<>();   // next entry to send to each peer 
    private final Map<String, Long> matchIndex = new HashMap<>();  // highest entry known to be on each peer
    private String currentLeader = null; // for each peer to know who is the leader
    private final PersistentState storage;
    private final RaftLog log = new RaftLog();

    // we change this values ONLY in a synchronized block or method
    private long commitIndex = 0;   // highest index known to be committed
    private long lastApplied = 0;   // highest index applied to the state machine
    private long currentTerm = 0;
    private String votedFor = null;
    private Role role = Role.FOLLOWER;

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    private ScheduledFuture<?> electionTimer;

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

    public void start() {
        resetElectionTimer();
        scheduler.scheduleAtFixedRate(this::sendHeartbeatsIfLeader, 0, 500, TimeUnit.MILLISECONDS);
    }

    // Election Timer
    private synchronized void resetElectionTimer() {
        if (electionTimer != null) electionTimer.cancel(false);
        long timeout = 1500 + ThreadLocalRandom.current().nextLong(1500);  //1.5–3 s while testing
        electionTimer = scheduler.schedule(this::startElection, timeout, TimeUnit.MILLISECONDS);
    }

    // if the term is a newer i have to become a follower
    private synchronized void stepDownIfNewerTerm(long term) {
        if(term > currentTerm) {
            currentTerm = term;
            votedFor = null;
            storage.save(currentTerm, votedFor);
            role = Role.FOLLOWER;
            System.out.println("[" + this.nodeId +"] term is " + term + ": becoming follower");
        }
    }

    //Handlers to see who can vote
    public synchronized VoteResponse onRequestVote(VoteRequest req) {
        VoteResponse rsp;
        stepDownIfNewerTerm(req.getTerm()); //if i dont have the new term i need to get it and become a follower
        if(req.getTerm() < currentTerm) { // i have a smaller term from the candidate i cant vote
            rsp = VoteResponse.newBuilder()
                    .setTerm(currentTerm)
                    .setVoteGranted(false)
                    .build();
        }  else if ((votedFor == null || votedFor.equals(req.getCandidateId())) && log.isUpToDate(req.getLastLogTerm(), req.getLastLogIndex()))  { //i did not vote so i need to vote 
            votedFor = req.getCandidateId();
            storage.save(currentTerm, votedFor);
            resetElectionTimer();
            rsp = VoteResponse.newBuilder()
                    .setTerm(currentTerm)
                    .setVoteGranted(true)
                    .build();

        } else { // already voted
            rsp = VoteResponse.newBuilder()
                    .setTerm(currentTerm)
                    .setVoteGranted(false)
                    .build();
        }
        return rsp;
    }

    public synchronized AppendResponse onAppendEntries(AppendRequest req) {
        AppendResponse rsp;
        if(req.getTerm() < currentTerm) { // Old Leader
            rsp = AppendResponse.newBuilder()
                    .setTerm(currentTerm)
                    .setSuccess(false)
                    .build();
        } else { // if a candidate sees a leader in the same term he needs to resign
            stepDownIfNewerTerm(req.getTerm());
            if (role != Role.FOLLOWER) System.out.println("[" + nodeId + "] term " + req.getTerm() + ": following leader " + req.getLeaderId());
            role = Role.FOLLOWER;
            currentLeader = req.getLeaderId();
            resetElectionTimer();
            boolean ok = log.tryAppend(req.getPrevLogIndex(), req.getPrevLogTerm(), req.getEntriesList());
            if(ok && req.getLeaderCommit() > commitIndex) { 
                commitIndex = Math.min(req.getLeaderCommit(), req.getPrevLogIndex() + req.getEntriesCount());
            }
            applyCommitted();
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
     * The startElection method is called when the peers don't have a leader(they don't get any heartbeat)
     * and the need to elect one. 
     * **/
    private void startElection() {
        long electionTerm;
        long lastLogIndex;
        long lastLogTerm;
        synchronized (this) {
            if (role == Role.LEADER) { resetElectionTimer(); return; } // we have a leader
            currentTerm++;
            role = Role.CANDIDATE; 
            votedFor = nodeId; 
            storage.save(currentTerm, votedFor); // save the variables in memory(.txt)
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
                if(resp.getTerm() > electionTerm) { //if he has a newer term step him down
                    stepDownIfNewerTerm(resp.getTerm());
                    return;
                } else if(resp.getVoteGranted()) { // vote granted
                    votes++;
                }
            } catch (StatusRuntimeException e) { // did not make it in time
                channels.get(peer.getKey()).resetConnectBackoff();
            }
        }

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
        if(resp.getTerm() > currentTerm) { // has a newer term step him down
            stepDownIfNewerTerm(resp.getTerm());
        } else if(role != Role.LEADER || currentTerm != req.getTerm()) {
            //this answer is stale (is far behind or lost the request)
        } else if(resp.getSuccess()) { // if he got it save it and prepare for commit
            matchIndex.put(peer, req.getPrevLogIndex() + req.getEntriesCount());
            nextIndex.put(peer,matchIndex.get(peer) + 1);
            advanceCommitIndex();

        } else {
             nextIndex.put(peer,Math.max(1, nextIndex.get(peer) - 1)); // walk back, retry next round
        }
    }

    // must be called while holding the lock (from a synchronized method)
    private void applyCommitted() {
        while(lastApplied < commitIndex) {
            lastApplied++;
            System.out.println("[" + nodeId + "] " + "apply " + lastApplied + ": " + log.get(lastApplied).getCommand());
        }
    }

    // must be called while holding the lock
    private void becomeLeader() {
        role = Role.LEADER;
        currentLeader = nodeId;
        for (String peer : peers.keySet()) {
            nextIndex.put(peer, log.lastIndex() + 1); //assume they have everything we have
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
            if (count >= majority && log.termAt(n) == currentTerm) {
                 commitIndex = n; 
                 applyCommitted(); 
                 return;
            }          
        }
    }
}