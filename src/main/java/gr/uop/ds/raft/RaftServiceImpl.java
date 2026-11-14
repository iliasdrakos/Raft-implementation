package gr.uop.ds.raft;

import gr.uop.ds.raft.proto.AppendRequest;
import gr.uop.ds.raft.proto.AppendResponse;
import gr.uop.ds.raft.proto.CommandRequest;
import gr.uop.ds.raft.proto.CommandResponse;
import gr.uop.ds.raft.proto.RaftServiceGrpc;
import gr.uop.ds.raft.proto.VoteRequest;
import gr.uop.ds.raft.proto.VoteResponse;
import io.grpc.stub.StreamObserver;


/**
 * The gRPC side of a Raft node: receives the two Raft RPCs from the other nodes
 * and forwards them to RaftNode, which holds all the logic and the state.
 */
public class RaftServiceImpl extends RaftServiceGrpc.RaftServiceImplBase {
    private final RaftNode node;
    public RaftServiceImpl(RaftNode node) { this.node = node; }

    /**
     * Called by gRPC when a candidate asks for our vote.
     * We must always answer exactly once (onNext + onCompleted) otherwise the candidate
     * waits until its deadline and counts it as a failed call.
     */
    @Override
    public void requestVote(VoteRequest req, StreamObserver<VoteResponse> out) {
        out.onNext(node.onRequestVote(req));
        out.onCompleted();
    }

    /**
     * Called by gRPC when a leader sends a heartbeat or log entries.
     * Like requestVote, we must always answer, even with success = false, because the
     * response carries our term.
     */
    @Override
    public void appendEntries(AppendRequest req, StreamObserver<AppendResponse> out) {
        out.onNext(node.onAppendEntries(req));
        out.onCompleted();
    }

    /**    
     * Called by gRPC when a follower forwards a client command to us. 
     * If we are the leader we append it; otherwise we answer "not the leader"
     */
    @Override
    public void clientCommand(CommandRequest req, StreamObserver<CommandResponse> out) {
        out.onNext(node.submit(req.getCommand()));
        out.onCompleted();
    }

    /**    
     * Called by gRPC when another node asks if we WOULD vote for it (Pre-Vote), 
     * so that a node that was cut off cannot disrupt the cluster when it comes back.
     */
    @Override 
    public void preVote(VoteRequest req, StreamObserver<VoteResponse> out) {
        out.onNext(node.onPreVote(req));
        out.onCompleted();
    }
}