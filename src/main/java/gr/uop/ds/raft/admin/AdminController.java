package gr.uop.ds.raft.admin;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import java.util.Map;
import gr.uop.ds.raft.FaultInjector;
import gr.uop.ds.raft.RaftNode;
import java.util.concurrent.atomic.AtomicLong;
import gr.uop.ds.raft.admin.dto.CommandRequest;
import gr.uop.ds.raft.admin.dto.CommandResult;
import gr.uop.ds.raft.proto.CommandResponse;
import gr.uop.ds.raft.admin.dto.EventsResponse;
import gr.uop.ds.raft.admin.dto.FaultRequest;
import gr.uop.ds.raft.admin.dto.StatusResponse;

/**
 * The HTTP adapter of one node: the same things as the CLI (STATUS, EVENTS, FAULT ...), as JSON.
 * It only calls public methods of the core; no Raft rule is written here.
 */
@RestController
@CrossOrigin // the dashboard page comes from one node and also asks the other two (another port = another origin)
public class AdminController {

    private final RaftNode node;
    private final FaultInjector faults;
    private final long startedAt = System.currentTimeMillis(); // one value per process
    private final String clientId = "dashboard-" + System.currentTimeMillis(); // one session per process, like the CLI
    private final AtomicLong seq = new AtomicLong(); // Tomcat runs requests in parallel threads: two clicks must never get the same seq

    public AdminController(RaftNode node, FaultInjector faults) {
        this.node = node;
        this.faults = faults;
    }

    @GetMapping("/status")
    public StatusResponse status() {
        return new StatusResponse(startedAt, node.snapshot(), faults.describe());
    }

    @GetMapping("/events")
    public EventsResponse events(@RequestParam(defaultValue = "0") long after) {
        return new EventsResponse(startedAt, node.eventsAfter(after));
    }

    @PostMapping("/fault")
    public StatusResponse fault(@RequestBody FaultRequest req) {
        if (req.type() == null) throw badRequest("missing \"type\"");
        switch (req.type().toLowerCase()) {
            case "isolate" -> {
                if (req.value() != null) {
                    faults.isolateFor(req.value());   // like FAULT isolate 20: for 20 seconds
                } else {
                    faults.isolate();                 // until "off"
                }
            }
            case "drop" -> faults.drop(required(req));
            case "delay" -> faults.delay(required(req));
            case "noreply" -> faults.dropReplies();
            case "off" -> faults.off();
            default -> throw badRequest("unknown type \"" + req.type() + "\": isolate, drop, delay, noreply or off");
        }
        return status(); // the page sees the new state at once
    }

    @PostMapping("/command")
    public CommandResult command(@RequestBody CommandRequest req) {
        if (req.command() == null || req.command().isBlank()) throw badRequest("missing \"command\"");
        String[] parts = req.command().trim().split("\\s+");
        parts[0] = parts[0].toUpperCase();
        int needed = switch (parts[0]) {
            case "PUT" -> 3;                  // PUT key value
            case "GET", "DEL", "INCR" -> 2;   // GET key
            default -> throw badRequest("unknown command \"" + parts[0] + "\": PUT, GET, DEL or INCR");
        };
        if (parts.length < needed) {
            throw badRequest(parts[0] + " needs " + (needed == 3 ? "a key and a value" : "a key"));
        }
        String command = String.join(" ", parts);
        CommandResponse r = node.execute(command, clientId, seq.incrementAndGet()); // a follower forwards it to the leader
        return new CommandResult(r.getSuccess(), r.getResult(), r.getLeaderId(), r.getIndex());
    }

    @PostMapping("/crash")
    public Map<String, String> crash() {
        // answer first, die a moment later: if we halted here, the page would get no answer at all
        Thread killer = new Thread(() -> {
            try {
                Thread.sleep(200);
            } catch (InterruptedException ignored) {
            }
            System.out.println("[" + node.snapshot().id() + "] CRASH from the dashboard");
            Runtime.getRuntime().halt(1); // like the CRASH command: no shutdown, no flush, like a power cut
        });
        killer.setDaemon(true);
        killer.start();
        return Map.of("result", "crashing in 200 ms");
    }

    // drop and delay need a number
    private static int required(FaultRequest req) {
        if (req.value() == null) throw badRequest("\"" + req.type() + "\" needs a \"value\"");
        return req.value();
    }

    private static ResponseStatusException badRequest(String why) {
        return new ResponseStatusException(HttpStatus.BAD_REQUEST, why);
    }
}
