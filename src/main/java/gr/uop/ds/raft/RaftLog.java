package gr.uop.ds.raft;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import gr.uop.ds.raft.proto.LogEntry;

public class RaftLog {

    private final List<LogEntry> Logs = new ArrayList<>();

    public long lastIndex() {
        if(!Logs.isEmpty()) {
            return Logs.getLast().getIndex();
        } else {
            return 0;
        }
    }

    public long lastTerm() {
        if(!Logs.isEmpty()) {
            return Logs.getLast().getTerm();
        } else {
            return 0;
        }
    }

    public long termAt(long index) {
        if (index == 0) return 0;
        if (index > lastIndex()) return -1;
        return Logs.get(Math.toIntExact(index - 1)).getTerm();
    }

    public LogEntry get(long index) {
        if (index == 0) return null;
        if (index > lastIndex()) return null;
        return Logs.get(Math.toIntExact(index - 1));
    }

    public void append(LogEntry e) {
        if(e.getIndex() != lastIndex() + 1) {
              throw new IllegalArgumentException("expected index " + (lastIndex() + 1) + " but got " + e.getIndex());
        } else {
            Logs.add(e);
        }
    }

    public void truncateFrom(long index) {
        Iterator<LogEntry> it = Logs.iterator();

        while(it.hasNext()) {
            LogEntry element = it.next(); 
            if(element.getIndex() >= index) {
                it.remove();
            }
        }
    }

    public List<LogEntry> entriesFrom(long index) {
        List<LogEntry> temp_logs = new ArrayList<>();
        
        for (LogEntry element : Logs) { 
            if(element.getIndex() >= index) {
                temp_logs.add(element);
            }
        }
        return temp_logs;
    }

}
