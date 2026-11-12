package gr.uop.ds.raft;

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

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
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

    // Infrastructure: not part of the Raft algorithm, just what we need to run it
    private final String nodeId;
    private final PersistentState storage; // writes currentTerm and votedFor to data/<nodeId>/state.txt
    private final Map<String, RaftServiceGrpc.RaftServiceBlockingStub> peers = new LinkedHashMap<>(); // one gRPC stub per other node
    private final Map<String, ManagedChannel> channels = new LinkedHashMap<>(); // the connection behind each stub
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2); // runs the election timer and the heartbeats
    private ScheduledFuture<?> electionTimer; // the pending startElection(); resetElectionTimer() replaces it

    // Logging only: not used by the algorithm
    private final NodeLogger out;
    private final Map<String, Boolean> peerUp = new HashMap<>(); // last known reachability, so we print only changes
    private long electionTimeoutMs;
    private long lastHeartbeatAt = System.currentTimeMillis();

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
        this.out = new NodeLogger(nodeId);
        this.storage = new PersistentState(nodeId);
        this.log = new RaftLog(new LogStorage(Path.of("data", nodeId)));
        PersistentState.State saved = storage.load();
        this.currentTerm = saved.currentTerm();
        this.votedFor = saved.votedFor();
        for (var e : cluster.entrySet()) {
            if (e.getKey().equals(nodeId)) continue;
            ManagedChannel ch = ManagedChannelBuilder.forAddress("localhost", e.getValue())
                    .usePlaintext().build();
            channels.put(e.getKey(), ch);
            peers.put(e.getKey(), RaftServiceGrpc.newBlockingStub(ch));
        }
        print(DISK, "loaded state.txt: term=" + currentTerm + ", votedFor=" + (votedFor == null ? "none" : votedFor));
        print(DISK, "loaded log.bin: " + log.lastIndex() + " entries (last " + log.lastIndex() + "/t" + log.lastTerm() + ")");
        print(STATE, "started as FOLLOWER, peers=" + peers.keySet());
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
        electionTimeoutMs = timeout;
        electionTimer = scheduler.schedule(this::startElection, timeout, TimeUnit.MILLISECONDS);
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
            currentLeader = null;
            print(STATE, oldRole + " -> FOLLOWER: " + from + " has newer term " + term + " (ours was " + oldTerm + ")");
            print(DISK, "saved state.txt: term=" + currentTerm + ", votedFor=none");
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
            lastHeartbeatAt = System.currentTimeMillis();
            resetElectionTimer(); // the leader is alive, so we must not start an election
            long ourPrevTerm = log.termAt(req.getPrevLogIndex());
            long conflictAt = firstConflict(req.getEntriesList());
            // consistency check: accept the entries only if our log has prevLogIndex with prevLogTerm (Log Matching)
            boolean ok = log.tryAppend(req.getPrevLogIndex(), req.getPrevLogTerm(), req.getEntriesList());
            if (!ok) show = true;
            if (show) print(RECV, "<- " + leader + " " + NodeLogger.fmt(req));
            if (!ok && ourPrevTerm == -1) {
                print(REPL, "REJECT: we don't have entry " + req.getPrevLogIndex() + " yet (our log ends at " + log.lastIndex() + ")");
            } else if (!ok) {
                print(REPL, "REJECT: term mismatch at " + req.getPrevLogIndex() + " (ours t" + ourPrevTerm + ", leader's t" + req.getPrevLogTerm() + ")");
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
     * Entry point for client commands.
     * A follower only tells the client who the leader is.
     * The leader appends the command to its own log; it is NOT committed yet.
     * The heartbeats replicate it, and it is applied once a majority has it.
     * @param command the client's command, e.g. "PUT x 5"
     */
    public synchronized void submit(String command) {
        if (role != Role.LEADER) {
            print(CLIENT, "\"" + command + "\" rejected: not the leader, the leader is "
                    + (currentLeader == null ? "unknown" : currentLeader));
            return;
        }
        long index = log.lastIndex() + 1;
        log.append(LogEntry.newBuilder().setIndex(index).setTerm(currentTerm).setCommand(command).build());
        print(CLIENT, "\"" + command + "\" appended at index " + index + " (term " + currentTerm
                + "), saved to log.bin, waiting for a majority");
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
            long silence = System.currentTimeMillis() - lastHeartbeatAt;
            print(ELECT, "election timeout (" + electionTimeoutMs + " ms): no heartbeat from "
                    + (currentLeader == null ? "any leader" : "leader " + currentLeader) + " for " + silence + " ms");
            Role oldRole = role;
            currentTerm++;
            role = Role.CANDIDATE;
            votedFor = nodeId;
            currentLeader = null;
            storage.save(currentTerm, votedFor); // writing in the disk
            lastLogIndex = log.lastIndex();
            lastLogTerm  = log.lastTerm();
            electionTerm = currentTerm;
            print(STATE, oldRole + " -> CANDIDATE: starting election for term " + electionTerm + ", voted for myself");
            print(DISK, "saved state.txt: term=" + currentTerm + ", votedFor=" + nodeId);
            resetElectionTimer();   //if the Voting ends with a tie then we try again later
        }

        int votes = 1;   // my vote
        List<String> granted = new ArrayList<>(List.of(nodeId));
        List<String> denied = new ArrayList<>();
        List<String> unreachable = new ArrayList<>();
        for (var peer : peers.entrySet()) { // we sent to every peer the VoteRequest to vote
            String id = peer.getKey();
            try {
                VoteRequest req = VoteRequest.newBuilder()
                        .setTerm(electionTerm)
                        .setCandidateId(nodeId)
                        .setLastLogIndex(lastLogIndex)
                        .setLastLogTerm(lastLogTerm)
                        .build();
                print(SEND, "-> " + id + " " + NodeLogger.fmt(req));
                VoteResponse resp = peer.getValue().withDeadlineAfter(300, TimeUnit.MILLISECONDS).requestVote(req);
                markPeer(id, true, null);
                print(RECV, "<- " + id + " " + NodeLogger.fmt(resp));
                if(resp.getTerm() > electionTerm) { //if he has a newer term step me down
                    stepDownIfNewerTerm(resp.getTerm(), id);
                    return;
                } else if(resp.getVoteGranted()) { // vote granted
                    votes++;
                    granted.add(id);
                } else {
                    denied.add(id);
                }
            } catch (StatusRuntimeException e) { // did not make it in time
                // the peer is down or too slow, so no vote from it; resetConnectBackoff() lets us reconnect at once when it comes back
                unreachable.add(id);
                markPeer(id, false, e.getStatus().getCode().toString());
                print(ERROR, "RequestVote to " + id + " failed: " + e.getStatus().getCode());
                channels.get(id).resetConnectBackoff();
            }
        }

        // we released the lock during the RPCs, so re-check that we are still a candidate in the same term
        synchronized (this) {
            int majority = (peers.size() + 1) / 2 + 1;
            int clusterSize = peers.size() + 1;
            if(role == Role.CANDIDATE && currentTerm == electionTerm && votes >= majority) { // checking who got the majority of the votes
                print(ELECT, "WON term " + electionTerm + " with " + votes + "/" + clusterSize + " votes " + granted);
                becomeLeader();
            } else if (role == Role.CANDIDATE && currentTerm == electionTerm) {
                print(ELECT, "NO MAJORITY in term " + electionTerm + ": " + votes + "/" + clusterSize + " votes " + granted
                        + ", need " + majority
                        + (denied.isEmpty() ? "" : ", denied by " + denied + " (split vote or our log is behind)")
                        + (unreachable.isEmpty() ? "" : ", unreachable " + unreachable)
                        + " -> new election after the next timeout");
            } else {
                print(ELECT, "election of term " + electionTerm + " is over: we are now " + role
                        + (currentLeader == null ? "" : ", leader is " + currentLeader));
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
                if (req.getEntriesCount() > 0 || out.isVerbose()) {
                    print(SEND, "-> " + id + " " + NodeLogger.fmt(req));
                }
                AppendResponse resp = peer.getValue().withDeadlineAfter(300, TimeUnit.MILLISECONDS).appendEntries(req);
                markPeer(id, true, null);
                onAppendResponse(id, req, resp);
            } catch (StatusRuntimeException e) {
                // the follower is down or slow: we simply retry on the next heartbeat (500 ms)
                markPeer(id, false, e.getStatus().getCode().toString());
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
        if (req.getEntriesCount() > 0 || !resp.getSuccess() || out.isVerbose()) {
            print(RECV, "<- " + peer + " " + NodeLogger.fmt(resp));
        }
        if(resp.getTerm() > currentTerm) { // has a newer term step me down
            stepDownIfNewerTerm(resp.getTerm(), peer);
        } else if(role != Role.LEADER || currentTerm != req.getTerm()) {
            // its a response from an old term or we are not the leader
            if (out.isVerbose()) print(REPL, "ignored a stale answer from " + peer + " (our request was for term " + req.getTerm() + ")");
        } else if(resp.getSuccess()) { // if he got it save it and prepare for commit
            matchIndex.put(peer, req.getPrevLogIndex() + req.getEntriesCount());
            nextIndex.put(peer,matchIndex.get(peer) + 1);
            if (req.getEntriesCount() > 0) {
                print(REPL, peer + " has our log up to " + matchIndex.get(peer)
                        + " (matchIndex=" + matchIndex.get(peer) + ", nextIndex=" + nextIndex.get(peer) + ")");
            }
            advanceCommitIndex();

        } else {
             long oldNext = nextIndex.get(peer);
             nextIndex.put(peer,Math.max(1, nextIndex.get(peer) - 1)); // walk back, retry next round
             print(REPL, peer + " rejected prev=" + req.getPrevLogIndex() + "/t" + req.getPrevLogTerm()
                     + ": nextIndex " + oldNext + " -> " + nextIndex.get(peer) + ", we retry from further back");
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
        }
    }

    /**
     * Called once we won the election. We don't know yet how far each follower's log goes,
     * so we start optimistic (nextIndex = our last index + 1) and walk back on every failure.
     */
    // must be called while holding the lock
    private void becomeLeader() {
        Role oldRole = role;
        role = Role.LEADER;
        currentLeader = nodeId;
        for (String peer : peers.keySet()) {
            nextIndex.put(peer, log.lastIndex() + 1); // assume they have everything we have
            matchIndex.put(peer, 0L); //we have confirmed nothing yet
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
}