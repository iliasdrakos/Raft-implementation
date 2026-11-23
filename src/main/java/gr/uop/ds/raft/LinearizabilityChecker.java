package gr.uop.ds.raft;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Checks a history written by RaftClient for linearizability based onWing & Gong, with Lowe's cache.
 *
 * The question: is there ONE order of the operations that
 *   (1) explains every answer (a GET returns the value of the last PUT before it in that order), and
 *   (2) respects real time (if A ended before B started, A comes before B)?
 *
 * Every key is a separate register and is checked on its own.
 * - PUT ok:      it happened, it must be in the order, somewhere between its start and its end
 * - PUT unknown: it may have happened at any moment after its start, or never: it is optional
 * - PUT fail:    it surely did not happen: left out
 * - GET ok:      must be in the order and must see the current value
 * - GET not ok:  changed nothing and has no value to check: left out
 *
 * Initial value: the cluster may not be empty when the client starts (log.bin of an earlier run).
 * A GET that returns a value that no PUT in this history wrote can only have seen the value the key
 * had before the history started. So such values are the candidates for the initial value of the key;
 * we try each one (and "(not found)" when there is none) and the key is linearizable if one works.
 *
 * Usage: LinearizabilityChecker history.csv [more.csv ...]   exit code 0 = all linearizable, 1 = violation
 */
public class LinearizabilityChecker {

    private static final String NOT_FOUND = "(not found)"; // what a GET returns for a key that was never written
    private static final long NEVER = Long.MAX_VALUE;     // end time of an unknown PUT: it has no end

    private record Op(int id, boolean put, String value, String result, long start, long end, String client) { }

    public static void main(String[] args) throws IOException {
        if (args.length == 0) {
            System.out.println("usage: LinearizabilityChecker history.csv [more.csv ...]");
            System.exit(2);
        }
        boolean allOk = true;
        for (String file : args) {
            allOk &= checkFile(file);
        }
        System.exit(allOk ? 0 : 1);
    }

    private static boolean checkFile(String file) throws IOException {
        Map<String, List<Op>> byKey = new TreeMap<>();
        Map<String, Set<String>> writtenByKey = new TreeMap<>(); // every value a PUT of this history tried to write, fail included
        int skippedFail = 0, unknownPuts = 0, lines = 0;
        long t0 = Long.MAX_VALUE;

        try (BufferedReader in = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = in.readLine()) != null) {
                if (line.startsWith("#") || line.startsWith("client,") || line.isBlank()) continue;
                // client,op,key,value,start_ns,end_ns,status,result  (result may be empty)
                String[] f = line.split(",", -1);
                if (f.length < 8) continue;
                lines++;
                String client = f[0], op = f[1], key = f[2], value = f[3], status = f[6], result = f[7];
                long start = Long.parseLong(f[4]);
                long end = Long.parseLong(f[5]);
                t0 = Math.min(t0, start);
                boolean put = op.equals("PUT");
                if (put) writtenByKey.computeIfAbsent(key, k -> new HashSet<>()).add(value);

                if (status.equals("fail") || (!put && !status.equals("ok"))) {
                    skippedFail++;               // surely not applied, or a read without an answer
                    continue;
                }
                if (put && status.equals("unknown")) {
                    unknownPuts++;
                    end = NEVER;                 // may take effect at any time after its start
                }
                List<Op> ops = byKey.computeIfAbsent(key, k -> new ArrayList<>());
                ops.add(new Op(ops.size(), put, value, result, start, end, client));
            }
        }

        System.out.printf("%s: %d operations (%d left out as fail, %d unknown PUTs)%n", file, lines, skippedFail, unknownPuts);
        boolean fileOk = true;
        for (Map.Entry<String, List<Op>> e : byKey.entrySet()) {
            long began = System.nanoTime();
            List<String> initials = initialCandidates(e.getValue(), writtenByKey.getOrDefault(e.getKey(), Set.of()));
            Result r = null;
            String initial = null;
            for (String candidate : initials) {
                Result tried = checkKey(e.getValue(), candidate);
                if (r == null || tried.ok || tried.maxDepth > r.maxDepth) {
                    r = tried;
                    initial = candidate;
                }
                if (r.ok) break;
            }
            long ms = (System.nanoTime() - began) / 1_000_000;
            if (!initial.equals(NOT_FOUND) || initials.size() > 1) {
                System.out.printf("  %-4s note: GETs read %s that no PUT of this history wrote -> initial value taken as %s%n",
                        e.getKey(), initials, initial);
            }
            if (r.ok) {
                System.out.printf("  %-4s %5d ops  linearizable  (%d ms)%n", e.getKey(), e.getValue().size(), ms);
            } else {
                fileOk = false;
                Op o = r.stuck;
                System.out.printf("  %-4s %5d ops  VIOLATION     (%d ms)%n", e.getKey(), e.getValue().size(), ms);
                System.out.printf("        at most %d ops could be put in order; stuck at: %s %s %s by %s, t=%.3f-%s s%n",
                        r.maxDepth, o.put ? "PUT" : "GET", e.getKey(), o.put ? o.value : "-> " + o.result, o.client,
                        (o.start - t0) / 1e9, o.end == NEVER ? "?" : String.format("%.3f", (o.end - t0) / 1e9));
            }
        }
        System.out.println(fileOk ? "RESULT: linearizable" : "RESULT: NOT linearizable");
        return fileOk;
    }

    private record Result(boolean ok, int maxDepth, Op stuck) { }

    /** Values read by a GET that no PUT of this key wrote: what the key may have held before the history. */
    private static List<String> initialCandidates(List<Op> ops, Set<String> written) {
        // a value written by a PUT of this history (even a failed one) cannot be the initial value
        Set<String> foreign = new java.util.TreeSet<>();
        for (Op o : ops) if (!o.put && !written.contains(o.result)) foreign.add(o.result);
        if (foreign.isEmpty()) return List.of(NOT_FOUND);
        return new ArrayList<>(foreign);
    }

    /** Wing & Gong search with Lowe's cache, for one register. */
    private static Result checkKey(List<Op> ops, String initial) {
        // one "call" event per operation, one "return" event per operation that has an end;
        // sorted by time, calls before returns at the same instant.
        List<long[]> ev = new ArrayList<>();               // {time, type (0 = call, 1 = return), op id}
        for (Op o : ops) {
            ev.add(new long[] {o.start, 0, o.id});
            if (o.end != NEVER) ev.add(new long[] {o.end, 1, o.id});
        }
        ev.sort(Comparator.<long[]>comparingLong(a -> a[0]).thenComparingLong(a -> a[1]));
        int n = ev.size();
        boolean[] isReturn = new boolean[n];
        int[] opOf = new int[n];
        int[] retPos = new int[ops.size()];
        java.util.Arrays.fill(retPos, -1);
        for (int k = 0; k < n; k++) {
            isReturn[k] = ev.get(k)[1] == 1;
            opOf[k] = (int) ev.get(k)[2];
            if (isReturn[k]) retPos[opOf[k]] = k;
        }

        // the events as a doubly linked list, so that a linearized operation can be lifted out and put back
        int[] next = new int[n], prev = new int[n];
        for (int k = 0; k < n; k++) {
            next[k] = k + 1;
            prev[k] = k - 1;
        }
        int[] head = {n == 0 ? n : 0};

        int mustPlace = 0;                                 // every operation that has an end must be in the order
        for (Op o : ops) if (o.end != NEVER) mustPlace++;

        BitSet linearized = new BitSet(ops.size());
        Set<Object> cache = new HashSet<>();               // (linearized set, value) pairs already tried
        Deque<int[]> stack = new ArrayDeque<>();           // {call event, index of the value before it}
        List<String> values = new ArrayList<>();           // the value before each step, so we can go back
        String state = initial;                            // the value of the key before the first operation
        int placed = 0, maxDepth = 0;
        Op stuck = null;
        int k = head[0];

        while (true) {
            if (placed == mustPlace) return new Result(true, stack.size(), null);
            if (k >= n) return new Result(true, stack.size(), null); // only optional (unknown) operations are left

            if (!isReturn[k]) {
                Op o = ops.get(opOf[k]);
                String newState;
                boolean fits;
                if (o.put) {
                    newState = o.value;                    // a PUT always fits and sets the value
                    fits = true;
                } else {
                    newState = state;                      // a GET fits only if it saw the current value
                    fits = o.result.equals(state);
                }
                if (fits) {
                    BitSet with = (BitSet) linearized.clone();
                    with.set(o.id);
                    if (cache.add(List.of(with, newState))) {
                        // put this operation next in the order
                        values.add(state);
                        stack.push(new int[] {k, values.size() - 1});
                        linearized.set(o.id);
                        state = newState;
                        if (o.end != NEVER) placed++;
                        lift(k, retPos[o.id], next, prev, head, n);
                        if (stack.size() > maxDepth) maxDepth = stack.size();
                        k = head[0];
                        continue;
                    }
                }
                k = next[k];                               // try the next operation that may go next
            } else {
                // a return event: this operation ended before everything after it started,
                // so it should already be in the order; it is not, so the last choice was wrong
                if (stack.size() >= maxDepth) stuck = ops.get(opOf[k]);
                if (stack.isEmpty()) return new Result(false, maxDepth, stuck);
                int[] top = stack.pop();
                int callK = top[0];
                Op o = ops.get(opOf[callK]);
                state = values.remove(top[1]);
                linearized.clear(o.id);
                if (o.end != NEVER) placed--;
                unlift(callK, retPos[o.id], next, prev, head, n);
                k = next[callK];                           // try the next choice after the one we undid
            }
        }
    }

    private static void lift(int call, int ret, int[] next, int[] prev, int[] head, int n) {
        remove(call, next, prev, head, n);
        if (ret >= 0) remove(ret, next, prev, head, n);
    }

    private static void unlift(int call, int ret, int[] next, int[] prev, int[] head, int n) {
        if (ret >= 0) restore(ret, next, prev, head, n);
        restore(call, next, prev, head, n);
    }

    private static void remove(int x, int[] next, int[] prev, int[] head, int n) {
        int p = prev[x], q = next[x];
        if (p >= 0) next[p] = q; else head[0] = q;
        if (q < n) prev[q] = p;
    }

    private static void restore(int x, int[] next, int[] prev, int[] head, int n) {
        int p = prev[x], q = next[x];
        if (p >= 0) next[p] = x; else head[0] = x;
        if (q < n) prev[q] = x;
    }
}
