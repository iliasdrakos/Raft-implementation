package gr.uop.ds.raft;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import gr.uop.ds.raft.proto.LogEntry;


/**
 * The replicated log of a Raft node.
 * Indexes start at 1; index 0 is a virtual entry with term 0, so that
 * prevLogIndex = 0 always matches (no special case for an empty log).
 * Not thread-safe: always used while holding the RaftNode lock.
 */
public class RaftLog {

    private final List<LogEntry> entries = new ArrayList<>(); // entry with index i is stored at position i - 1

    /** Index of the last entry, or 0 if the log is empty. */
    public long lastIndex() {
        if(!entries.isEmpty()) {
            return entries.getLast().getIndex();
        } else {
            return 0;
        }
    }

    /** Term of the last entry, or 0 if the log is empty. */
    public long lastTerm() {
        if(!entries.isEmpty()) {
            return entries.getLast().getTerm();
        } else {
            return 0;
        }
    }

    /** Term of the entry at index, 
     * 0 for the virtual entry 0, 
     * -1 if we don't have it. */
    public long termAt(long index) {
        if (index == 0) return 0;
        if (index > lastIndex()) return -1;
        return entries.get(Math.toIntExact(index - 1)).getTerm();
    }

    /**
     * @param index to get the LogEntry
     * @return the log entry if found else null 
     */
    public LogEntry get(long index) {
        if (index == 0) return null;
        if (index > lastIndex()) return null;
        return entries.get(Math.toIntExact(index - 1));
    }

    /**
     * @param e is the Log entry that we save in our list IF it has the same index as the lastIndex + 1 (the next one)
     * @throws IllegalArgumentException if the index is not lastIndex() + 1
     */
    public void append(LogEntry e) {
        if(e.getIndex() != lastIndex() + 1) {
              throw new IllegalArgumentException("expected index " + (lastIndex() + 1) + " but got " + e.getIndex());
        } else {
            entries.add(e);
        }
    }

    /**
     * Follower side of AppendEntries.
     * First checks that our log has an entry at prevLogIndex with prevLogTerm.
     * This means our logs are identical up to prevLogIndex.
     * Then, for each new entry:
     *  - missing here: append it
     *  - same index, different term (conflict): delete it and everything after, then append
     *  - identical: do nothing. A late or duplicate message must NOT delete newer entries,
     *    because they may already be committed.
     * @param prevLogIndex index of the entry just before the new ones, in the leader's log
     * @param prevLogTerm  term of that entry
     * @param newEntries      the new entries (empty for a heartbeat)
     * @return false if the consistency check fails, true otherwise
     */
    public boolean tryAppend(long prevLogIndex, long prevLogTerm, List<LogEntry> newEntries) {
        if(termAt(prevLogIndex) != prevLogTerm) return false; // if the terms don't match return 

        for(LogEntry element: newEntries) { // for every entry
            long existingTerm = termAt(element.getIndex());
            if (existingTerm == -1) {                          // we don't have this position yet, append it
                append(element);
            } else if(existingTerm != element.getTerm()) { // if the terms don't match
                truncateFrom(element.getIndex()); // used when our entries conflict with the leader's
                append(element); // then append the new ones
            }
        }
        return true;
    }

    // this method deletes the entries from a specific index until the end of the list
    public void truncateFrom(long index) {
        Iterator<LogEntry> it = entries.iterator();

        while(it.hasNext()) {
            LogEntry element = it.next(); 
            if(element.getIndex() >= index) {
                it.remove();
            }
        }
    }

    // returns a copy used by the leader to send entries to a follower
    public List<LogEntry> entriesFrom(long index) {
        List<LogEntry> temp_logs = new ArrayList<>();

        for (LogEntry element : entries) { 
            if(element.getIndex() >= index) {
                temp_logs.add(element);
            }
        }
        return temp_logs;
    }

    /**
     * Election restriction.
     * A candidate's log is at least as up-to-date as ours if its last entry has a higher term,
     * or the same term and an index at least as large.
     * Voting only for such candidates guarantees Leader Completeness:
     * every new leader already has all committed entries.
     */
    public boolean isUpToDate(long candidateLastTerm, long candidateLastIndex) {
        long lastTerm = lastTerm();
        if (candidateLastTerm > lastTerm) {
            return true;
        } else if (candidateLastTerm == lastTerm) {
            return candidateLastIndex >= lastIndex();
        } else {
            return false;
        }
    }

}
