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

}
