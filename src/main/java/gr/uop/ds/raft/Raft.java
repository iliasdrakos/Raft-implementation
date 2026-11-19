package gr.uop.ds.raft;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
                    if(arg != null && (arg.equalsIgnoreCase("on") || arg.equalsIgnoreCase("off"))) {
                        boolean on = arg.equalsIgnoreCase("on");
                        node.setPreVote(on);
                        System.out.println("[" + nodeId + "] pre-vote " + (on ? "ON" : "OFF"));
                    } else {
                        System.out.println("For the PREVOTE command you need to type ON|OFF");
                    }
                   
                    
                }
                case "FAULT" -> handleFault(nodeId, faults, arg);
                case "CRASH" -> Runtime.getRuntime().halt(1);
                case "BENCH" -> {
                try {
                    int n = arg == null ? 100 : Integer.parseInt(arg.trim());
                    if (n < 1) throw new NumberFormatException();
                    runBench(node, n);
                } catch (NumberFormatException e) {
                    System.out.println("usage: BENCH [n]   (n >= 1, default 100)");
                }
}
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

    /** sends n PUTs one after the other and prints the latency the client sees. */
    private static void runBench(RaftNode node, int n) {
        List<Long> all = new ArrayList<>();   // latency of every request in ms, failures included
        int ok = 0, unknown = 0, rejected = 0 , retries = 0;
        long start = System.nanoTime();       // a clock that never jumps, made for measuring durations
        for (int i = 0; i < n; i++) {
            String cmd = "PUT bench" + i + " " + i;
            long t0 = System.nanoTime();
            CommandResponse r;
            while (true) {
                r = node.execute(cmd);
                boolean unknownOutcome = r.getResult().startsWith("timeout") || r.getResult().startsWith("unknown outcome");
                boolean surelyNotApplied = !r.getSuccess() && !unknownOutcome;   // "no leader known", "not the leader"
                if (!surelyNotApplied || (System.nanoTime() - t0) / 1_000_000 > 5000) break;   // done, or give up after 5 s
                retries++;
                try {
                    Thread.sleep(50);   // the cluster may be electing a leader: wait a little and ask again
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            all.add((System.nanoTime() - t0) / 1_000_000);   // includes the waiting
            if (r.getSuccess()) {
                ok++;
            } else if (r.getResult().startsWith("timeout") || r.getResult().startsWith("unknown outcome")) {
                unknown++;
            } else {
                rejected++;   // gave up after 5 s
            }

        }
        double seconds = (System.nanoTime() - start) / 1e9;
        Collections.sort(all);
        double avg = all.stream().mapToLong(Long::longValue).average().orElse(0);
        System.out.printf("[%s] BENCH n=%d: ok=%d, unknown=%d, retries=%d, rejected=%d in %.1f s, throughput=%.1f PUT/s%n",
                node.getNodeId(), n, ok, unknown, retries, rejected, seconds, ok / seconds);
        System.out.printf("        latency (ms): avg=%.1f  p50=%d  p95=%d  p99=%d  max=%d%n",
                avg, percentile(all, 0.50), percentile(all, 0.95), percentile(all, 0.99), all.get(all.size() - 1));
    }

    // the smallest value that at least q of the samples are below or equal to (q in (0,1])
    private static long percentile(List<Long> sorted, double q) {
        int rank = (int) Math.ceil(q * sorted.size());
        return sorted.get(Math.max(rank, 1) - 1);
    }

    private static void printHelp() {
        System.out.println("commands: PUT key value | DEL key | GET key | STATUS | LOG | VERBOSE on|off | HELP | FAULT [isolate|drop N|delay MS|off] | CRASH | STATS [reset] | BENCH [iterations]");
    }
}