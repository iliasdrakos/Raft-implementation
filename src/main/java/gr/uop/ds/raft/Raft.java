package gr.uop.ds.raft;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Map;

import io.grpc.Server;
import io.grpc.ServerBuilder;


/**
 * Entry point of one Raft node.
 * Usage: { @code Raft <nodeId }, For now the cluster is fixed: node1..node3 on ports 50051..50053.
 * It starts the gRPC server, then the node, and reads commands from the terminal:
 *   GET key        answered from this node's own KvStore (on a follower the value may be stale)
 *   PUT key value  goes to the log through submit(); only the leader accepts it
 *   DEL key        same as PUT
 */
public class Raft {
    
    /**
     * The gRPC server starts BEFORE node.start(): as soon as our timers run we may send
     * or receive RPCs, so we must already be able to answer our peers.
     * @param args args[0] is the id of this node (default is "node1")
     */
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
            line = line.trim();
            if (line.isEmpty()) continue;

            String[] parts = line.split("\\s+", 2);
            if (parts[0].equalsIgnoreCase("GET")) {
                if (parts.length < 2) {
                    System.out.println("usage: GET key");
                } else {
                    String value = node.localGet(parts[1]);
                    System.out.println("[" + nodeId + "] " + parts[1] + " = "
                            + (value == null ? "(not found)" : value));
                }
            } else {
                node.submit(line);   // PUT OR DEL go through Raft
            }
        }
        server.awaitTermination();
    }
}