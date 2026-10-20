package gr.uop.ds.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

class KvStoreTest {

    @Test
    void putThenGet() {
        KvStore kv = new KvStore();
        assertEquals("OK", kv.apply("PUT x 5"));
        assertEquals("5", kv.get("x"));
    }

    @Test
    void putOverwrites() {
        KvStore kv = new KvStore();
        kv.apply("PUT x 1");
        kv.apply("PUT x 2");
        assertEquals("2", kv.get("x"));
    }

    @Test
    void deleteExistingAndMissing() {
        KvStore kv = new KvStore();
        kv.apply("PUT x 1");
        assertEquals("OK", kv.apply("DEL x"));
        assertNull(kv.get("x"));
        assertEquals("NOT_FOUND", kv.apply("DEL x"));
    }

    @Test
    void commandsAreCaseInsensitive() {
        KvStore kv = new KvStore();
        kv.apply("put y 3");
        assertEquals("3", kv.get("y"));
    }

    @Test
    void valueMayContainSpaces() {
        KvStore kv = new KvStore();
        kv.apply("PUT msg hello world");
        assertEquals("hello world", kv.get("msg"));
    }

    @Test
    void noopAndInvalidCommandsDoNotChangeState() {
        KvStore kv = new KvStore();
        assertEquals("OK", kv.apply("NOOP"));
        assertTrue(kv.apply("FOO bar").startsWith("ERR"));
        assertTrue(kv.apply("PUT onlykey").startsWith("ERR"));   // missing value
        assertTrue(kv.apply("DEL").startsWith("ERR"));           // missing key
        assertEquals(0, kv.size());
    }
}