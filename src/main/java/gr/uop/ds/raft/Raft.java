package gr.uop.ds.raft;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Map;

import gr.uop.ds.raft.proto.CommandResponse;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptors;


/**
 * Entry point of one Raft node.
 * Usage: { @code Raft <nodeId }, For now the cluster is fixed: node1..node3 on ports 50051..50053.
 * It starts the gRPC server, then the node, and reads commands from the terminal:
 *   GET key        answered from this node's own KvStore (on a follower the value may be stale)
 *   PUT key value  goes to the log through submit(); only the leader accepts it
 *   DEL key        same as PUT
 *   STATUS         the state of this node (role, term, leader, commit, peers)
 *   LOG            every entry of the log and whether it is committed
 *   VERBOSE on|off show or hide the empty heartbeats
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
        FaultInjector faults = new FaultInjector();
        RaftNode node = new RaftNode(nodeId, cluster,faults);
        Server server = ServerBuilder.forPort(port)
                .addService(ServerInterceptors.intercept(new RaftServiceImpl(node), faults))
                .build()
                .start();
        System.out.println("[" + nodeId + "] listening on port " + port);
        printHelp();
        node.start();
        // Temporary "client" to type commands in the leader's terminal, e.g. PUT x 5
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        String line;
        while ((line = in.readLine()) != null) {
            line = line.trim();
            if (line.isEmpty()) continue;

            String[] parts = line.split("\\s+", 2);
            String arg = parts.length > 1 ? parts[1] : null;   // what comes after the command, or null

            switch (parts[0].toUpperCase()) {
                case "GET" -> {
                    if (arg == null) {
                        System.out.println("usage: GET key");
                    } else {
                        String value = node.localGet(arg);
                        System.out.println("[" + nodeId + "] " + arg + " = " + (value == null ? "(not found)" : value));
                    }
                }
                case "PUT", "DEL" -> {
                    CommandResponse r = node.execute(line);   // PUT OR DEL go through Raft
                    if (r.getSuccess()) {
                        System.out.println("[" + nodeId + "] " + line + " -> " + r.getResult()
                                + " (committed at index " + r.getIndex() + " by leader " + r.getLeaderId() + ")");
                    } else {
                        System.out.println("[" + nodeId + "] " + line + " -> FAILED: " + r.getResult());
                    }
                }
                case "STATUS" -> System.out.println(node.status());
                case "LOG" -> System.out.println(node.logView());
                case "STATS" -> {
                    if ("reset".equalsIgnoreCase(arg)) {
                        node.resetStats();
                        System.out.println("[" + nodeId + "] stats reset");
                    } else {
                        System.out.println(node.statsReport());
                    }
                }
                case "VERBOSE" -> {
                    boolean on = arg == null || arg.equalsIgnoreCase("on");
                    node.setVerbose(on);
                    System.out.println("verbose " + (on ? "ON: empty heartbeats are shown" : "OFF"));
                }
                case "PREVOTE" -> {
                    boolean on = arg == null || arg.equalsIgnoreCase("on");
                    node.setPreVote(on);
                    System.out.println("[" + nodeId + "] pre-vote " + (on ? "ON" : "OFF"));
                }
                case "FAULT" -> handleFault(nodeId, faults, arg);
                case "CRASH" -> Runtime.getRuntime().halt(1);
                case "HELP" -> printHelp();
                default -> System.out.println("unknown command: " + parts[0] + " (type HELP)");
            }
        }
        server.awaitTermination();
    }

    // FAULT [isolate [seconds] | drop <percent> | delay <ms> | off]
    private static void handleFault(String nodeId, FaultInjector faults, String arg) {
        if (arg != null) {
            String[] f = arg.split("\\s+");   // e.g. ["drop", "30"]
            try {
                switch (f[0].toLowerCase()) {
                    case "isolate" -> {
                        if (f.length == 2) {
                            faults.isolateFor(Long.parseLong(f[1]));   // FAULT isolate 20
                        } else {
                            faults.isolate();                           // until FAULT off
                        }
                    }
                    case "off" -> faults.off();
                    case "drop" -> faults.drop(Integer.parseInt(f[1]));
                    case "delay" -> faults.delay(Long.parseLong(f[1]));
                    default -> System.out.println("usage: FAULT [isolate [seconds] | drop <percent> | delay <ms> | off]");
                }
            } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
                System.out.println("usage: FAULT [isolate [seconds] | drop <percent> | delay <ms> | off]");
            }
        }
        System.out.println("[" + nodeId + "] faults: " + faults.describe());
    }
    private static void printHelp() {
        System.out.println("commands: PUT key value | DEL key | GET key | STATUS | LOG | VERBOSE on|off | HELP | FAULT [isolate|drop N|delay MS|off] | CRASH | STATS [reset]");
    }
}