package gr.uop.ds.raft;

import java.util.HashMap;
import java.util.Map;

/**
 * The state machine of a Raft node: a simple key-value store. The Raft log
 * holds the history of commands; this map holds the current state that results
 * from applying the committed commands in log order. Every node applies the
 * same commands in the same order, so all nodes end up with the same map. Not
 * thread-safe: used while holding the RaftNode lock.
 */
public class KvStore {

    private final Map<String, String> kvmap = new HashMap<>(); // key -> current value
    private final Map<String, Long> lastSeq = new HashMap<>();      // client sessions: clientId -> last seq applied
    private final Map<String, String> lastResult = new HashMap<>(); // clientId -> the result of that seq

    /**
     * Applies one committed command and returns its result. Supported: PUT key
     * value, DEL key, NOOP. Never throws: an invalid command returns "ERR ..."
     * and changes nothing. This keeps the state machine deterministic; an
     * exception here would crash every node on the same log entry, and again on
     * every restart.
     *
     * @param command the command text of a committed log entry
     * @return "OK", "NOT_FOUND" or "ERR ..."
     */
    public String apply(String command) {
        String[] parts = command.trim().split("\\s+", 3);   // any amount of spaces between words
        String op = parts[0].toUpperCase();                  // "put" and "PUT" are the same
        switch (op) {
            case "NOOP":
                return "OK";
            case "GET":
                if (parts.length < 2) {
                    return "ERR usage: GET key";
                }
                String value = kvmap.get(parts[1]);   // just a read changes nothing, but runs at its place in the log order
                return value == null ? "(not found)" : value;
            case "PUT":
                if (parts.length < 3) {
                    return "ERR usage: PUT key value";
                }
                kvmap.put(parts[1], parts[2]);
                return "OK";
            case "DEL":
                if (parts.length < 2) {
                    return "ERR usage: DEL key";
                }
                return kvmap.remove(parts[1]) != null ? "OK" : "NOT_FOUND";
            case "INCR":
                if (parts.length < 2) {
                    return "ERR usage: INCR key";
                }
                long v;
                try {
                    v = Long.parseLong(kvmap.getOrDefault(parts[1], "0"));
                } catch (NumberFormatException e) {
                    return "ERR not a number: " + parts[1];
                }
                kvmap.put(parts[1], String.valueOf(v + 1));
                return String.valueOf(v + 1);
            default:
                return "ERR unknown command: " + op;
        }
    }

    /**
     * Current value of key, or null if it does not exist.
     */
    public String get(String key) {
        return kvmap.get(key);
    }

    /**
     * Number of keys currently stored.
     */
    public int size() {
        return kvmap.size();
    }

    /**
     * True if this was already applied, so it must not run again.
     */
    public boolean isDuplicate(String clientId, long seq) {
        if (clientId == null || clientId.isEmpty()) return false;
        Long last = lastSeq.get(clientId);
        return last != null && seq <= last;
    }

    /**
     * Applies a command at most once per (clientId, seq).
     * A command without a client id (NOOP, old log entries) is always applied.
     * The same seq again gets the stored result; an older seq gets an error,
     * because the client has already moved on to a newer command.
     */
    public String apply(String clientId, long seq, String command) {
        if (clientId == null || clientId.isEmpty()) return apply(command);
        Long last = lastSeq.get(clientId);
        if (last != null && seq == last) return lastResult.get(clientId);
        if (last != null && seq < last) return "ERR old request (seq " + seq + " < " + last + ")";
        String result = apply(command);
        lastSeq.put(clientId, seq);
        lastResult.put(clientId, result);
        return result;
    }

}
