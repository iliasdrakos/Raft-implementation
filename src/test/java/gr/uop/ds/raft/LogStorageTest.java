package gr.uop.ds.raft;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import gr.uop.ds.raft.proto.LogEntry;

class LogStorageTest {

    @TempDir
    Path dir;   // JUnit gives every test a new empty folder and deletes it afterwards

    private static LogEntry entry(long index, long term, String cmd) {
        return LogEntry.newBuilder().setIndex(index).setTerm(term).setCommand(cmd).build();
    }

    @Test
    void appendThenLoadReturnsTheSameEntries() throws Exception {
        LogStorage storage = new LogStorage(dir);
        storage.append(List.of(entry(1, 1, "PUT x 1"), entry(2, 1, "PUT y 2")));
        storage.append(List.of(entry(3, 2, "DEL x")));

        List<LogEntry> loaded = new LogStorage(dir).load();   // a NEW object, like after a restart

        assertEquals(3, loaded.size());
        assertEquals("DEL x", loaded.get(2).getCommand());
        assertEquals(2, loaded.get(2).getTerm());
    }

    @Test
    void loadWithoutFileReturnsEmptyList() throws Exception {
        List<LogEntry> loaded = new LogStorage(dir).load(); // must return an empty list
        assertEquals(0, loaded.size());
    }

    @Test
    void brokenTailIsDropped() throws Exception {
        LogStorage storage = new LogStorage(dir);
        storage.append(List.of(entry(1, 1, "PUT x 1"), entry(2, 1, "PUT y 2")));
        // simulate a crash in the middle of an append: garbage bytes at the end of the file
        Files.write(dir.resolve("log.bin"), new byte[] {50, 1, 2, 3}, StandardOpenOption.APPEND);

        List<LogEntry> loaded = storage.load(); // must return only the 2 good entries
        assertEquals(2, loaded.size());
        storage.append(List.of(entry(3, 1, "PUT z 3")));
        List<LogEntry> reloaded = new LogStorage(dir).load();
        assertEquals(3, reloaded.size());
        assertEquals("PUT z 3", reloaded.get(2).getCommand());
    }
}