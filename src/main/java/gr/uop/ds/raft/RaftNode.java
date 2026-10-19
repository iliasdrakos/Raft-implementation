package gr.uop.ds.raft;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import gr.uop.ds.raft.proto.AppendRequest;
import gr.uop.ds.raft.proto.AppendResponse;
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
    private final PersistentState storage;

    // we change this values ONLY in a synchronized block or method
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
        } else if(votedFor == null || votedFor.equals(req.getCandidateId())) { //i did not vote so i need to vote 
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
        } else { // if a candidate sees a leader in the same term he needs to resigned
            stepDownIfNewerTerm(req.getTerm());
            if (role != Role.FOLLOWER) System.out.println("[" + nodeId + "] term " + req.getTerm() + ": following leader " + req.getLeaderId());
            role = Role.FOLLOWER;
            resetElectionTimer();
            rsp = AppendResponse.newBuilder()
                    .setTerm(currentTerm)
                    .setSuccess(true)
                    .build();

        }
        return rsp;
    }

    // ELECTIONS
    private void startElection() {
        long electionTerm;
        synchronized (this) {
            if (role == Role.LEADER) { resetElectionTimer(); return; }
            currentTerm++;
            role = Role.CANDIDATE; 
            votedFor = nodeId; 
            storage.save(currentTerm, votedFor);
            electionTerm = currentTerm;
            System.out.println("[" + nodeId + "] term " + electionTerm + ": becoming candidate");
            resetElectionTimer();   //if the Voting ends with a tie then we try again later
        }

        int votes = 1;   // my vote
        for (var peer : peers.entrySet()) {
            try {
                VoteRequest req = VoteRequest.newBuilder()
                        .setTerm(electionTerm)
                        .setCandidateId(nodeId)
                        .build();
                VoteResponse resp = peer.getValue().withDeadlineAfter(300, TimeUnit.MILLISECONDS).requestVote(req);
                if(resp.getTerm() > electionTerm) {
                    stepDownIfNewerTerm(resp.getTerm());
                    return;
                } else if(resp.getVoteGranted()) {
                    votes++;
                }
            } catch (StatusRuntimeException e) {
                channels.get(peer.getKey()).resetConnectBackoff();
            }
        }

        synchronized (this) {
            int majority = (peers.size() + 1) / 2 + 1;
            if(role == Role.CANDIDATE && currentTerm == electionTerm && votes >= majority) {
                role = Role.LEADER;
                System.out.println("[" + nodeId + "] term " + electionTerm + ": is the new leader with " + votes + " votes");
            }
        }
    }

    // Heartbeats (only the leader)
    private void sendHeartbeatsIfLeader() {
        long term;
        synchronized (this) {
            if (role != Role.LEADER) return;
            term = currentTerm;
        }
        for (var peer : peers.entrySet()) {
            try {
                AppendRequest req = AppendRequest.newBuilder()
                        .setTerm(term)
                        .setLeaderId(nodeId)
                        .build();
                AppendResponse resp = peer.getValue().withDeadlineAfter(300, TimeUnit.MILLISECONDS).appendEntries(req);
                stepDownIfNewerTerm(resp.getTerm());   // do we have any new leader;
            } catch (StatusRuntimeException e) {
                channels.get(peer.getKey()).resetConnectBackoff();
            }
        }
    }
}