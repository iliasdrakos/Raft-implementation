package gr.uop.ds.raft;

import java.util.List;

/**
 * A snapshot of one node's state at one moment, as plain data.
 * RaftNode fills it; every adapter decides how to show it:
 * the CLI prints it as text (STATUS), the dashboard will get it as JSON (GET /status).
 */
public record NodeStatus(
        String id,
        RaftNode.Role role,
        long term,
        String leader,               // null = no leader known
        String votedFor,             // null = no vote in this term
        long commitIndex,
        long lastApplied,
        long lastLogIndex,           // = the log length, as long as we have no snapshots
        long lastLogTerm,            // with lastLogIndex: decides who may win an election
        int kvKeys,
        boolean preVoteEnabled,
        boolean checkQuorumEnabled,
        boolean readIndexEnabled,
        String lastElection,         // result of our last vote or pre-vote round, "none" if there was none
        long leaderlessMs,           // how long we have been without a known leader (0 = we know one)
        List<PeerStatus> peers) {

    /** What this node knows about one other node. */
    public record PeerStatus(
            String id,
            Boolean up,              // null = we never talked to it
            long msSinceHeard,       // time since any message from it reached us; -1 = never
            long matchIndex,         // only on the leader; -1 otherwise
            long nextIndex,          // only on the leader; -1 otherwise
            long received) { }       // how many messages from it reached us since we started
}

