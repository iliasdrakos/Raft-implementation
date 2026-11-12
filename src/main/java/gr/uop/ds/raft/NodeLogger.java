package gr.uop.ds.raft;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

import gr.uop.ds.raft.proto.AppendRequest;
import gr.uop.ds.raft.proto.AppendResponse;
import gr.uop.ds.raft.proto.LogEntry;
import gr.uop.ds.raft.proto.VoteRequest;
import gr.uop.ds.raft.proto.VoteResponse;

/**
 * Prints every event of a Raft node as one line:
 *   time [node|term|role] KIND message
 * Empty heartbeats are hidden unless verbose mode is on (VERBOSE command),
 * because the leader sends them every 500 ms.
 * Colors can be turned off with -Draft.color=false.
 */
public class NodeLogger {

    public enum Kind {
        STATE("STATE ", "\u001B[1;33m"),
        ELECT("ELECT ", "\u001B[1;35m"),
        VOTE("VOTE  ", "\u001B[36m"),
        SEND("SEND  ", "\u001B[34m"),
        RECV("RECV  ", "\u001B[94m"),
        REPL("REPL  ", "\u001B[32m"),
        COMMIT("COMMIT", "\u001B[1;32m"),
        APPLY("APPLY ", "\u001B[92m"),
        CLIENT("CLIENT", "\u001B[97m"),
        PEER("PEER  ", "\u001B[33m"),
        DISK("DISK  ", "\u001B[90m"),
        ERROR("ERROR ", "\u001B[1;31m");

        private final String label;
        private final String color;

        Kind(String label, String color) {
            this.label = label;
            this.color = color;
        }
    }

    private static final String RESET = "\u001B[0m";
    private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
    private static final int MAX_ENTRIES_SHOWN = 4;

    private final String nodeId;
    private final boolean color = !"false".equalsIgnoreCase(System.getProperty("raft.color"));
    private volatile boolean verbose = Boolean.getBoolean("raft.verbose");

    public NodeLogger(String nodeId) {
        this.nodeId = nodeId;
    }

    public boolean isVerbose() {
        return verbose;
    }

    public void setVerbose(boolean verbose) {
        this.verbose = verbose;
    }

    public void log(Kind kind, long term, RaftNode.Role role, String message) {
        String header = String.format("%s [%s|T%-2d|%-9s] %s",
                LocalTime.now().format(TIME), nodeId, term, role, kind.label);
        if (color) {
            System.out.println(kind.color + header + RESET + " " + message);
        } else {
            System.out.println(header + " " + message);
        }
    }

    public static String fmt(VoteRequest r) {
        return "RequestVote{term=" + r.getTerm()
                + ", candidate=" + r.getCandidateId()
                + ", lastLog=" + r.getLastLogIndex() + "/t" + r.getLastLogTerm() + "}";
    }

    public static String fmt(VoteResponse r) {
        return "VoteResponse{term=" + r.getTerm() + ", granted=" + r.getVoteGranted() + "}";
    }

    public static String fmt(AppendRequest r) {
        return "AppendEntries{term=" + r.getTerm()
                + ", leader=" + r.getLeaderId()
                + ", prev=" + r.getPrevLogIndex() + "/t" + r.getPrevLogTerm()
                + ", entries=" + fmt(r.getEntriesList())
                + ", leaderCommit=" + r.getLeaderCommit() + "}";
    }

    public static String fmt(AppendResponse r) {
        return "AppendResponse{term=" + r.getTerm() + ", success=" + r.getSuccess() + "}";
    }

    public static String fmt(LogEntry e) {
        return e.getIndex() + ":t" + e.getTerm() + " \"" + e.getCommand() + "\"";
    }

    public static String fmt(List<LogEntry> entries) {
        if (entries.isEmpty()) return "[] (heartbeat)";
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < entries.size() && i < MAX_ENTRIES_SHOWN; i++) {
            if (i > 0) sb.append(", ");
            sb.append(fmt(entries.get(i)));
        }
        if (entries.size() > MAX_ENTRIES_SHOWN) {
            sb.append(", ... +").append(entries.size() - MAX_ENTRIES_SHOWN).append(" more");
        }
        return sb.append("]").toString();
    }
}