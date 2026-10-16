package gr.uop.ds.raft;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;

public class PersistentState {

    public record State(long currentTerm, String votedFor) {}

    private final Path file;
    private final Path tmp;

    public PersistentState(String nodeId) throws IOException {
        Path dir = Path.of("data", nodeId);          // e.g. data/node1/
        Files.createDirectories(dir);
        this.file = dir.resolve("state.txt");
        this.tmp  = dir.resolve("state.txt.tmp");
    }

    // Returns the saved state, or (0, null) on the very first start
    public State load() throws IOException {
        if (!Files.exists(file)) return new State(0, null);
        String[] lines = Files.readString(file, StandardCharsets.UTF_8).split("\n");
        long term = Long.parseLong(lines[0].trim());
        String voted = (lines.length > 1 && !lines[1].isBlank()) ? lines[1].trim() : null;
        return new State(term, voted);
    }

    // Write to a temp file, fsync, then atomically replace the old file.
    // A crash in the middle leaves either the old or the new file, never half of one.
    public void save(long currentTerm, String votedFor) {
        String content = currentTerm + "\n" + (votedFor == null ? "" : votedFor) + "\n";
        ByteBuffer buf = ByteBuffer.wrap(content.getBytes(StandardCharsets.UTF_8));
        try (FileChannel ch = FileChannel.open(tmp,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
            while (buf.hasRemaining()) ch.write(buf);
            ch.force(true);   // the data is really on disk, not just in the OS cache
            Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            // If we cannot persist, we must not continue
            throw new UncheckedIOException(e);
        }
    }
}