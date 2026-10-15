package gr.uop.ds.raft;

import gr.uop.ds.raft.proto.AppendRequest;
import gr.uop.ds.raft.proto.AppendResponse;
import gr.uop.ds.raft.proto.RaftServiceGrpc;
import gr.uop.ds.raft.proto.VoteRequest;
import gr.uop.ds.raft.proto.VoteResponse;
import io.grpc.stub.StreamObserver;

public class RaftServiceImpl extends RaftServiceGrpc.RaftServiceImplBase {

    private final String nodeId;
    private long currentTerm = 1;   // προς το παρόν σταθερό, θα αλλάξει με τις εκλογές

    public RaftServiceImpl(String nodeId) {
        this.nodeId = nodeId;
    }

    @Override
    public void requestVote(VoteRequest req, StreamObserver<VoteResponse> out) {
        System.out.println("[" + nodeId + "] RequestVote από " + req.getCandidateId()
                + " (term " + req.getTerm() + ")");

        VoteResponse resp = VoteResponse.newBuilder()
                .setTerm(currentTerm)
                .setVoteGranted(false)   // προς το παρόν δεν ψηφίζουμε κανέναν
                .build();
        out.onNext(resp);
        out.onCompleted();
    }

    @Override
    public void appendEntries(AppendRequest req, StreamObserver<AppendResponse> out) {
        if (req.getEntriesCount() == 0) {
            System.out.println("[" + nodeId + "] heartbeat from " + req.getLeaderId()
                    + " (term " + req.getTerm() + ")");
        } else {
            System.out.println("[" + nodeId + "] " + req.getEntriesCount() + " entries from " + req.getLeaderId());
        }
        AppendResponse resp = AppendResponse.newBuilder()
                        .setTerm(currentTerm)
                        .setSuccess(true)
                        .build();
        out.onNext(resp);
        out.onCompleted();
    }
}