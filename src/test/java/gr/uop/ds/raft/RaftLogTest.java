package gr.uop.ds.raft;

import gr.uop.ds.raft.proto.LogEntry;
import org.junit.jupiter.api.Test;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RaftLogTest {

    private static LogEntry entry(long index, long term, String cmd) {
        return LogEntry.newBuilder().setIndex(index).setTerm(term).setCommand(cmd).build();
    }

    @Test
    void emptyLog() {
        RaftLog log = new RaftLog();
        assertEquals(0, log.lastIndex());
        assertEquals(0, log.lastTerm());
        assertEquals(0, log.termAt(0));    // the virtual entry before the first one
        assertEquals(-1, log.termAt(1));   // we don't have index 1
    }

    @Test
    void appendAndRead() {
        RaftLog log = new RaftLog();
        log.append(entry(1, 1, "PUT x 1"));
        log.append(entry(2, 1, "PUT y 2"));
        log.append(entry(3, 2, "DEL x"));
        assertEquals(3, log.lastIndex());
        assertEquals(2, log.lastTerm());
        assertEquals(1, log.termAt(2));
        assertEquals("DEL x", log.get(3).getCommand());
    }

    @Test
    void appendWithWrongIndexIsRejected() {
        RaftLog log = new RaftLog();
        assertThrows(IllegalArgumentException.class, () -> log.append(entry(2, 1, "PUT x 1")));
    }

    @Test
    void truncateRemovesFromIndexToEnd() {
        RaftLog log = new RaftLog();
        log.append(entry(1, 1, "a"));
        log.append(entry(2, 1, "b"));
        log.append(entry(3, 1, "c"));
        log.truncateFrom(2);
        assertEquals(1, log.lastIndex());
        assertEquals(-1, log.termAt(2));
        log.append(entry(2, 3, "b2"));      // after truncation, index 2 is free again
        assertEquals(3, log.termAt(2));
    }

    @Test
    void entriesFromReturnsTheTail() {
        RaftLog log = new RaftLog();
        log.append(entry(1, 1, "a"));
        log.append(entry(2, 1, "b"));
        log.append(entry(3, 2, "c"));
        assertEquals(2, log.entriesFrom(2).size());
        assertEquals("b", log.entriesFrom(2).get(0).getCommand());
        assertTrue(log.entriesFrom(4).isEmpty());
    }

        @Test
    void tryAppendRejectsWhenPrevDoesNotMatch() {
        RaftLog log = new RaftLog();
        log.append(entry(1, 1, "a"));
        assertFalse(log.tryAppend(2, 1, List.of(entry(3, 1, "c"))));   // we don't have index 2
        assertFalse(log.tryAppend(1, 2, List.of(entry(2, 2, "b"))));   // index 1 has term 1, not 2
        assertEquals(1, log.lastIndex());                               // nothing changed
    }

    @Test
    void tryAppendOnEmptyLogWithPrevZero() {
        RaftLog log = new RaftLog();
        assertTrue(log.tryAppend(0, 0, List.of(entry(1, 1, "a"), entry(2, 1, "b"))));
        assertEquals(2, log.lastIndex());
    }

    @Test
    void tryAppendHeartbeatWithNoEntries() {
        RaftLog log = new RaftLog();
        log.append(entry(1, 1, "a"));
        assertTrue(log.tryAppend(1, 1, List.of()));
        assertEquals(1, log.lastIndex());
    }

    @Test
    void tryAppendReplacesConflictingTail() {
        RaftLog log = new RaftLog();
        log.append(entry(1, 1, "a"));
        log.append(entry(2, 1, "b"));   // 2 and 3 came from an old leader
        log.append(entry(3, 1, "c"));   // and were never committed
        assertTrue(log.tryAppend(1, 1, List.of(entry(2, 2, "B"))));
        assertEquals(2, log.lastIndex());               // old 2 and 3 are gone
        assertEquals("B", log.get(2).getCommand());
    }

    @Test
    void tryAppendDoesNotDeleteOnLateDuplicate() {
        RaftLog log = new RaftLog();
        log.append(entry(1, 1, "a"));
        log.append(entry(2, 1, "b"));
        log.append(entry(3, 1, "c"));
        // a delayed AppendEntries carrying only entry 2 arrives after we already have 2 and 3
        assertTrue(log.tryAppend(1, 1, List.of(entry(2, 1, "b"))));
        assertEquals(3, log.lastIndex());               // entry 3 must survive
    }

    @Test
    void tryAppendDoesNotDeleteOnLateDuplicateAfterTermChange() {
        RaftLog log = new RaftLog();
        log.append(entry(1, 1, "a"));
        log.append(entry(2, 2, "b"));
        log.append(entry(3, 2, "c"));
        assertTrue(log.tryAppend(1, 1, List.of(entry(2, 2, "b"))));
        assertEquals(3, log.lastIndex());               // entry 3 must survive
    }

    @Test
    void upToDateComparesLastTermFirst() {
        RaftLog log = new RaftLog();
        log.append(entry(1, 1, "a"));
        log.append(entry(2, 2, "b"));                 // my last entry: index 2, term 2
        assertTrue(log.isUpToDate(3, 1));             // higher last term wins, even if shorter
        assertFalse(log.isUpToDate(1, 10));           // lower last term loses, even if longer
    }

    @Test
    void upToDateWithSameLastTermComparesLength() {
        RaftLog log = new RaftLog();
        log.append(entry(1, 1, "a"));
        log.append(entry(2, 2, "b"));
        assertTrue(log.isUpToDate(2, 2));             // identical: ok
        assertTrue(log.isUpToDate(2, 3));             // longer: ok
        assertFalse(log.isUpToDate(2, 1));            // shorter: no
    }

    @Test
    void emptyCandidateLogLosesAgainstNonEmpty() {
        RaftLog log = new RaftLog();
        log.append(entry(1, 1, "a"));
        assertFalse(log.isUpToDate(0, 0));            // the scenario above: empty node3 must not win
        assertTrue(new RaftLog().isUpToDate(0, 0));   // two empty logs are equal
    }
}