package gr.uop.ds.raft;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import gr.uop.ds.raft.proto.CommandRequest;
import gr.uop.ds.raft.proto.CommandResponse;
import gr.uop.ds.raft.proto.RaftServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

/**
 * A separate client process for the linearizability check.
 *
 * Several threads run at the same time. Each thread is one client with its own session
 * (client id + seq) and sends one request at a time: random PUTs and GETs on a few keys,
 * so that operations of different clients overlap in time.
 *
 * Every operation is written to a history file (CSV), one line per operation:
 *   client,op,key,value,start_ns,end_ns,status,result
 * status is "ok", "fail" or "unknown". start_ns/end_ns come from System.nanoTime() of this process,
 * so all threads share the same clock. The checker only needs this file.
 *
 * Usage: RaftClient [threads] [seconds] [keys] [historyFile] or (defaults: 5 30 3 history.csv)
 */
public class RaftClient {

    private static final Map<String, Integer> CLUSTER = Map.of("node1", 50051, "node2", 50052, "node3", 50053);
    private static final List<String> NODES = List.of("node1", "node2", "node3");

    private static final long RPC_DEADLINE_MS = 2000; // the leader itself waits up to 1500 ms for a commit
    private static final long GIVE_UP_MS = 5000;      // per operation, retries included
    private static final long RETRY_PAUSE_MS = 50;
    // -Dclient.readAnyNode=true: every GET goes to a random node instead of the leader
    // (with -Draft.unsafeReads=true on the nodes this is the negative control: stale reads must be found)
    private static final boolean READ_ANY_NODE = Boolean.getBoolean("client.readAnyNode");

    private final Map<String, RaftServiceGrpc.RaftServiceBlockingStub> stubs = new HashMap<>();
    private final List<ManagedChannel> channels = new ArrayList<>();
    private final PrintWriter history;

    // live counters for the progress line
    private final AtomicInteger ok = new AtomicInteger();
    private final AtomicInteger fail = new AtomicInteger();
    private final AtomicInteger unknown = new AtomicInteger();
    private final List<Long> okLatencies = Collections.synchronizedList(new ArrayList<>());

    private RaftClient(String historyFile) throws IOException {
        for (String node : NODES) {
            ManagedChannel ch = ManagedChannelBuilder.forAddress("localhost", CLUSTER.get(node)).usePlaintext().build();
            channels.add(ch);
            stubs.put(node, RaftServiceGrpc.newBlockingStub(ch));
        }
        history = new PrintWriter(new FileWriter(historyFile, false));
        // lets us match nanoTime in the file with the wall clock in the node logs
        history.println("# started " + Instant.now() + " nanoTime=" + System.nanoTime());
        history.println("client,op,key,value,start_ns,end_ns,status,result");
        history.flush();
    }

    public static void main(String[] args) throws Exception {
        int threads = args.length > 0 ? Integer.parseInt(args[0]) : 5;
        int seconds = args.length > 1 ? Integer.parseInt(args[1]) : 30;
        int keys    = args.length > 2 ? Integer.parseInt(args[2]) : 3;
        String file = args.length > 3 ? args[3] : "history.csv";

        RaftClient client = new RaftClient(file);
        System.out.printf("RaftClient: %d clients, %d s, keys k0..k%d, reads %s, history -> %s%n",
                threads, seconds, keys - 1, READ_ANY_NODE ? "from ANY node" : "from the leader", file);

        long endAt = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
        List<Thread> workers = new ArrayList<>();
        for (int i = 1; i <= threads; i++) {
            int id = i;
            Thread t = new Thread(() -> client.runClient(id, keys, endAt), "client-" + i);
            workers.add(t);
            t.start();
        }

        // progress line every 2 s, so that we can watch what a fault does
        Thread progress = new Thread(() -> {
            long started = System.nanoTime();
            while (System.nanoTime() < endAt) {
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    return;
                }
                System.out.printf("[client] t=%2ds  ok=%d  fail=%d  unknown=%d%n",
                        (System.nanoTime() - started) / 1_000_000_000, client.ok.get(), client.fail.get(), client.unknown.get());
            }
        });
        progress.setDaemon(true);
        progress.start();

        for (Thread t : workers) t.join();
        client.close();
        client.printSummary(seconds);
    }

    /** One client: its own session, one operation at a time, until the time is up. */
    private void runClient(int id, int keys, long endAt) {
        String clientId = "c" + id + "-" + System.currentTimeMillis(); // a new session for every run
        Random rnd = new Random();
        long seq = 0;
        int written = 0;
        String leader = NODES.get((id - 1) % NODES.size()); // spread the first guesses over the nodes

        while (System.nanoTime() < endAt) {
            String key = "k" + rnd.nextInt(keys);
            boolean isPut = rnd.nextBoolean();
            String value = isPut ? "c" + id + "-" + (++written) : ""; // unique value: the checker knows which PUT a GET saw
            String command = isPut ? "PUT " + key + " " + value : "GET " + key;
            seq++;                                                   // a new number for every new operation

            long start = System.nanoTime();
            String status = "fail";
            String result = "";
            boolean sawUnknown = false;

            while (true) {
                String target = (!isPut && READ_ANY_NODE) ? NODES.get(rnd.nextInt(NODES.size())) : leader;
                Attempt a = send(target, command, clientId, seq);
                if (a.redirectTo != null && (isPut || !READ_ANY_NODE)) leader = a.redirectTo;
                if (a.outcome == Outcome.OK) {
                    status = "ok";
                    result = a.result;
                    break;
                }
                if (a.outcome == Outcome.UNKNOWN) sawUnknown = true;
                result = a.result;
                if ((System.nanoTime() - start) / 1_000_000 > GIVE_UP_MS) break; // give up on THIS operation
                pause();
            }
            long end = System.nanoTime();

            // a retry keeps the same seq, so a PUT is applied at most once; if we gave up after an
            // ambiguous answer, it may still be applied later: the checker must treat it as "unknown"
            if (!status.equals("ok") && sawUnknown && isPut) status = "unknown";
            record(clientId, isPut ? "PUT" : "GET", key, value, start, end, status, result);
        }
    }

    private enum Outcome { OK, SURELY_NOT, UNKNOWN }

    private record Attempt(Outcome outcome, String result, String redirectTo) { }

    /** One RPC to one node, classified the same way as in RaftNode.execute and BENCH. */
    private Attempt send(String node, String command, String clientId, long seq) {
        CommandRequest req = CommandRequest.newBuilder().setCommand(command).setClientId(clientId).setSeq(seq).build();
        try {
            CommandResponse r = stubs.get(node).withDeadlineAfter(RPC_DEADLINE_MS, TimeUnit.MILLISECONDS).clientCommand(req);
            if (r.getSuccess()) return new Attempt(Outcome.OK, r.getResult(), null);
            String res = r.getResult();
            if (res.startsWith("timeout") || res.startsWith("unknown outcome")) {
                return new Attempt(Outcome.UNKNOWN, res, null);
            }
            // "not the leader" and every other refusal: nothing was appended
            String hint = r.getLeaderId();
            String next = (hint != null && !hint.isEmpty() && !hint.equals(node)) ? hint : nextNode(node);
            return new Attempt(Outcome.SURELY_NOT, res, next);
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == Status.Code.UNAVAILABLE) {
                // the node is down: try the next one
                return new Attempt(Outcome.SURELY_NOT, "UNAVAILABLE " + node, nextNode(node));
            }
            // DEADLINE_EXCEEDED and the rest: the request may have arrived
            return new Attempt(Outcome.UNKNOWN, e.getStatus().getCode() + " " + node, nextNode(node));
        }
    }

    private static String nextNode(String node) {
        return NODES.get((NODES.indexOf(node) + 1) % NODES.size());
    }

    private static void pause() {
        try {
            Thread.sleep(RETRY_PAUSE_MS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private synchronized void record(String clientId, String op, String key, String value,
                                     long start, long end, String status, String result) {
        String safe = result.replace(",", ";"); // keep the CSV simple
        history.println(clientId + "," + op + "," + key + "," + value + "," + start + "," + end + "," + status + "," + safe);
        history.flush(); // a line is safe on disk even if the client is killed
        switch (status) {
            case "ok" -> {
                ok.incrementAndGet();
                okLatencies.add((end - start) / 1_000_000);
            }
            case "unknown" -> unknown.incrementAndGet();
            default -> fail.incrementAndGet();
        }
    }

    private void close() throws InterruptedException {
        history.close();
        for (ManagedChannel ch : channels) {
            ch.shutdown().awaitTermination(2, TimeUnit.SECONDS);
        }
    }

    private void printSummary(int seconds) {
        List<Long> lat = new ArrayList<>(okLatencies);
        Collections.sort(lat);
        int total = ok.get() + fail.get() + unknown.get();
        System.out.printf("[client] done: %d operations in %d s (%.1f ops/s): ok=%d, fail=%d, unknown=%d%n",
                total, seconds, (double) total / seconds, ok.get(), fail.get(), unknown.get());
        if (!lat.isEmpty()) {
            System.out.printf("[client] latency of ok operations (ms): p50=%d  p95=%d  p99=%d  max=%d%n",
                    pct(lat, 0.50), pct(lat, 0.95), pct(lat, 0.99), lat.get(lat.size() - 1));
        }
    }

    // nearest-rank percentile, same as in Raft.java
    private static long pct(List<Long> sorted, double p) {
        int rank = (int) Math.ceil(p * sorted.size());
        return sorted.get(Math.max(0, rank - 1));
    }
}
