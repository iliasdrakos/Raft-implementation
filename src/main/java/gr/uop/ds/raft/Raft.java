package gr.uop.ds.raft;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Map;

import io.grpc.Server;
import io.grpc.ServerBuilder;

public class Raft {

    public static void main(String[] args) throws Exception {
        Map<String, Integer> cluster = Map.of("node1", 50051, "node2", 50052, "node3", 50053);
        String nodeId = args.length > 0 ? args[0] : "node1";
        int port = cluster.get(nodeId);

        RaftNode node = new RaftNode(nodeId, cluster);
        Server server = ServerBuilder.forPort(port)
                .addService(new RaftServiceImpl(node))
                .build()
                .start();
        System.out.println("[" + nodeId + "] listening on port " + port);
        node.start();
        // Temporary "client" to type commands in the leader's terminal, e.g. PUT x 5
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        String line;
        while ((line = in.readLine()) != null) {
            if (!line.isBlank()) node.submit(line.trim());
        }
        server.awaitTermination();
    }
}