package gr.uop.ds.raft.admin;

import java.util.Map;

import org.springframework.boot.Banner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import gr.uop.ds.raft.FaultInjector;
import gr.uop.ds.raft.RaftNode;

/**
 * Starts the HTTP adapter (Spring Boot) of one node, next to its gRPC server and its CLI.
 * The node and the fault injector already exist (Raft.main created them), so we do not let
 * Spring create them: we hand the same objects to Spring as beans, and the controller gets them injected.
 */
@SpringBootApplication // scans only this package (gr.uop.ds.raft.admin): the core has no Spring annotations
public class DashboardServer {

    public static ConfigurableApplicationContext start(int port, RaftNode node, FaultInjector faults) {
        SpringApplication app = new SpringApplication(DashboardServer.class);
        app.setBannerMode(Banner.Mode.OFF);
        app.setLogStartupInfo(false);
        app.setDefaultProperties(Map.of(
                "server.port", port,
                "server.address", "127.0.0.1",   // FAULT and CRASH must be reachable only from this computer
                "logging.level.root", "WARN"));  // keep the terminal for the Raft log
        app.addInitializers(ctx -> {
            ctx.getBeanFactory().registerSingleton("raftNode", node);
            ctx.getBeanFactory().registerSingleton("faultInjector", faults);
        });
        app.setDefaultProperties(Map.of(
            "server.port", port,
            "server.address", "127.0.0.1",   // FAULT and CRASH must be reachable only from this computer
            "server.error.include-message", "always", // a 400 says why, e.g. "drop" needs a "value"
            "logging.level.root", "WARN"));  // keep the terminal for the Raft log
        return app.run();
    }
}
