package gr.uop.ds.raft;

import gr.uop.ds.raft.proto.LogEntry;
import org.junit.jupiter.api.Test;

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
}