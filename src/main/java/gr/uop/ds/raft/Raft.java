package gr.uop.ds.raft;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import gr.uop.ds.raft.proto.AppendRequest;
import gr.uop.ds.raft.proto.AppendResponse;
import gr.uop.ds.raft.proto.RaftServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.StatusRuntimeException;

public class Raft {

    public static void main(String[] args) throws Exception {
        //default variables
        String nodeId = "node1";
        int port = 50051;

        if (args.length == 2) {
            nodeId = args[0];
            port = Integer.parseInt(args[1]);
        }

        //creating the service
        Server server = ServerBuilder.forPort(port)
                .addService(new RaftServiceImpl(nodeId))
                .build()
                .start();
        System.out.println("[" + nodeId + "] listening on port " + port);

        if (nodeId.equals("node1")) {
            final String leaderId = nodeId;
            int[] peerPorts = {50052, 50053};
            ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor();

            for (int peerPort : peerPorts) {
                ManagedChannel channel = ManagedChannelBuilder
                        .forAddress("localhost", peerPort)
                        .usePlaintext()
                        .build();
                RaftServiceGrpc.RaftServiceBlockingStub stub = RaftServiceGrpc.newBlockingStub(channel);

                timer.scheduleAtFixedRate(() -> {
                    try {
                        AppendRequest req = AppendRequest.newBuilder()
                            .setTerm(1)
                            .setLeaderId(leaderId)
                            .build();     
                        AppendResponse resp = stub.withDeadlineAfter(200, TimeUnit.MILLISECONDS).appendEntries(req);
                        System.out.println("[node1] -> port" + peerPort + ": Success = " + resp.getSuccess());
                    } catch (StatusRuntimeException e) {
                        System.out.println("[node1] -> port" + peerPort + ": Error = " + e.getStatus().getCode());
                    }
                }, 0, 1000, TimeUnit.MILLISECONDS);
            }
        }

        server.awaitTermination();
    }
}