package gr.uop.ds.raft;

import java.util.HashMap;
import java.util.Map;

/**
 * Counters for the fault injection experiments (the STATS command).
 * Each node counts what IT saw: elections, step-downs, how long it had no leader,
 * and, while it is the leader, how long a client command needs to be committed.
 * "STATS reset" starts a new measurement, so every experiment is counted on its own.
 */
public final class Stats {

    private long startedAt;
    private int electionsStarted;
    private int electionsWon;
    private int leaderStepDowns;
    private int preVoteFailed;

    // periods without a known leader
    private long leaderlessSince;      // -1 = we know a leader right now
    private int leaderlessPeriods;
    private long leaderlessTotalMs;
    private long leaderlessMaxMs;

    // commit latency of client commands (only on the leader)
    private final Map<Long, Long> appendedAt = new HashMap<>();
    private int commits;
    private long commitLatencySumMs;
    private long commitLatencyMaxMs;

    public Stats() {
        reset(false);
    }

    public synchronized void reset(boolean leaderKnownNow) {
        long now = System.currentTimeMillis();
        startedAt = now;
        electionsStarted = 0;
        electionsWon = 0;
        leaderStepDowns = 0;
        leaderlessSince = leaderKnownNow ? -1 : now;
        leaderlessPeriods = 0;
        leaderlessTotalMs = 0;
        leaderlessMaxMs = 0;
        appendedAt.clear();
        commits = 0;
        commitLatencySumMs = 0;
        commitLatencyMaxMs = 0;
        preVoteFailed = 0;
    }

    public synchronized void electionStarted() {
        electionsStarted++;
    }

    public synchronized void electionWon() {
        electionsWon++;
    }

    public synchronized void steppedDown() {
        leaderStepDowns++;
    }

    public synchronized void preVoteFailed() {
        preVoteFailed++;
    }

    /** We stopped knowing a leader. sinceMs = the last moment we still had one. */
    public synchronized void leaderLost(long sinceMs) {
        if (leaderlessSince == -1) {
            leaderlessSince = Math.max(sinceMs, startedAt);
        }
    }

    /** We know a leader again (we follow one, or we became the leader). */
    public synchronized void leaderFound() {
        if (leaderlessSince != -1) {
            long period = System.currentTimeMillis() - leaderlessSince;
            leaderlessPeriods++;
            leaderlessTotalMs += period;
            leaderlessMaxMs = Math.max(leaderlessMaxMs, period);
            leaderlessSince = -1;
        }
    }

    /** The leader appended a client command at this index. */
    public synchronized void appended(long index) {
        appendedAt.put(index, System.currentTimeMillis());
    }

    /** The entry at this index was committed and applied. */
    public synchronized void applied(long index) {
        Long t = appendedAt.remove(index);
        if (t != null) {
            long latency = System.currentTimeMillis() - t;
            commits++;
            commitLatencySumMs += latency;
            commitLatencyMaxMs = Math.max(commitLatencyMaxMs, latency);
        }
    }

    public synchronized String report(String nodeId, long currentTerm) {
        long now = System.currentTimeMillis();
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("------------- stats of %s (last %.1f s) -------------%n",
                nodeId, (now - startedAt) / 1000.0));
        sb.append(String.format(" current term        : %d%n", currentTerm));
        sb.append(String.format(" elections started   : %d%n", electionsStarted));
        sb.append(String.format(" elections won       : %d%n", electionsWon));
        sb.append(String.format(" leader step-downs   : %d%n", leaderStepDowns));
        sb.append(String.format(" PreVote failed      : %d%n", preVoteFailed));
        if (leaderlessPeriods == 0) {
            sb.append(" periods w/o leader  : 0\n");
        } else {
            sb.append(String.format(" periods w/o leader  : %d (total %.2f s, longest %.2f s, average %.2f s)%n",
                    leaderlessPeriods, leaderlessTotalMs / 1000.0, leaderlessMaxMs / 1000.0,
                    leaderlessTotalMs / 1000.0 / leaderlessPeriods));
        }
        if (leaderlessSince != -1) {
            sb.append(String.format(" NOW without leader  : for %.2f s%n", (now - leaderlessSince) / 1000.0));
        }
        if (commits == 0) {
            sb.append(" commits as leader   : 0\n");
        } else {
            sb.append(String.format(" commits as leader   : %d (latency average %d ms, max %d ms)%n",
                    commits, commitLatencySumMs / commits, commitLatencyMaxMs));
        }
        sb.append("------------------------------------------------------");
        return sb.toString();
    }
}
