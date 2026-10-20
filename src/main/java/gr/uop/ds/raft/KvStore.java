package gr.uop.ds.raft;

import java.util.HashMap;
import java.util.Map;

public class KvStore {

    private final Map<String,String> kvmap = new HashMap<>();

        public String apply(String command) {
        String[] parts = command.trim().split("\\s+", 3);   // any amount of spaces between words
        String op = parts[0].toUpperCase();                  // "put" and "PUT" are the same
        switch (op) {
            case "NOOP":
                return "OK";
            case "PUT":
                if (parts.length < 3) return "ERR usage: PUT key value";
                kvmap.put(parts[1], parts[2]);
                return "OK";
            case "DEL":
                if (parts.length < 2) return "ERR usage: DEL key";
                return kvmap.remove(parts[1]) != null ? "OK" : "NOT_FOUND";
            default:
                return "ERR unknown command: " + op;
        }
    }

    public String get(String key) {
        return kvmap.get(key);
    }

    public int size() {
        return kvmap.size();
    }

}
