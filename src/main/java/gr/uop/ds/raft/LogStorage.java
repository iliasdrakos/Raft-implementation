package gr.uop.ds.raft;

import java.io.BufferedInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;

import com.google.protobuf.InvalidProtocolBufferException;

import gr.uop.ds.raft.proto.LogEntry;

public class LogStorage {

    private final Path file;
    private final Path tmp;

    public LogStorage(Path dir) throws IOException {
        Files.createDirectories(dir);
        this.file = dir.resolve("log.bin");
        this.tmp = dir.resolve("log.bin.tmp");
    }

    public void append(List<LogEntry> newEntries) {
        try (FileOutputStream out = new FileOutputStream(file.toFile(), true)) { 
            writeAll(out, newEntries);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public List<LogEntry> load() throws IOException {
        List<LogEntry> result = new ArrayList<>();
        if (!Files.exists(file)) return result;
        try (InputStream in = new BufferedInputStream(Files.newInputStream(file))) {
            while (true) {
                LogEntry entry = LogEntry.parseDelimitedFrom(in);
                if(entry == null) break;
                else result.add(entry);
            }
        } catch (InvalidProtocolBufferException e) {
            System.out.println("[WARNING] We kept " + result.size() + " good entries");
            rewrite(result);
        }
        return result;
    }

    public void rewrite(List<LogEntry> all) {
        try (FileOutputStream out = new FileOutputStream(tmp.toFile())) {
            writeAll(out, all);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        
        // here the tmp file is closed, so we can move it
        try {
            Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        
    }

    private void writeAll(FileOutputStream out, List<LogEntry> entries) {
        try {
            for(LogEntry entry : entries) {
                entry.writeDelimitedTo(out); // using the writeDelimitedTo method so we can have this in our file [length][entry 1][length][entry 2][length][entry 3] and so on
            }
            out.getChannel().force(true);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}