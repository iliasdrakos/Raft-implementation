package gr.uop.ds.raft;

import gr.uop.ds.raft.proto.AppendRequest;
import gr.uop.ds.raft.proto.AppendResponse;
import gr.uop.ds.raft.proto.RaftServiceGrpc;
import gr.uop.ds.raft.proto.VoteRequest;
import gr.uop.ds.raft.proto.VoteResponse;
import io.grpc.stub.StreamObserver;

public class RaftServiceImpl extends RaftServiceGrpc.RaftServiceImplBase {
    private final RaftNode node;
    public RaftServiceImpl(RaftNode node) { this.node = node; }

    @Override
    public void requestVote(VoteRequest req, StreamObserver<VoteResponse> out) {
        out.onNext(node.onRequestVote(req));
        out.onCompleted();
    }

    @Override
    public void appendEntries(AppendRequest req, StreamObserver<AppendResponse> out) {
        out.onNext(node.onAppendEntries(req));
        out.onCompleted();
    }
}