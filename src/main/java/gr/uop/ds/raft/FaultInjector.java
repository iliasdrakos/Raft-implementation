package gr.uop.ds.raft;

import java.util.concurrent.ThreadLocalRandom;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

/**
 * Fault injection for experiments.
 * It sits between this node and the network as two gRPC interceptors:
 * every RPC that leaves this node or reaches it
 * passes through here and can be dropped or delayed.
 * Controlled from the terminal with the FAULT command.
 */
public class FaultInjector implements ClientInterceptor, ServerInterceptor {

    private volatile int dropPercent = 0;   // 100 = the node is isolated
    private volatile long delayMs = 0;
    private volatile boolean dropReplies = false; // our requests arrive, but every answer to us is lost

    public void isolate() {
        dropPercent = 100;
        delayMs = 0;
    }

    public void drop(int percent) {
        dropPercent = Math.max(0, Math.min(100, percent));
    }

    public void delay(long ms) {
        delayMs = Math.max(0, ms);
    }

    public void dropReplies() {
        off();
        dropReplies = true;
    }

    public void off() {
        dropPercent = 0;
        delayMs = 0;
        dropReplies = false;
    }

    public String describe() {
        if (dropReplies) return "NO REPLIES: our RPCs reach the other nodes, but every answer to us is lost";
        if (dropPercent == 0 && delayMs == 0) return "no faults";
        if (dropPercent == 100) return "ISOLATED: every RPC to or from this node is dropped";
    
        return "dropping " + dropPercent + "% of RPCs, delaying each RPC that reaches this node by " + delayMs + " ms";
    }

    private boolean shouldDrop() {
        int p = dropPercent;
        return p >= 100 || (p > 0 && ThreadLocalRandom.current().nextInt(100) < p);
    }

    private void sleepDelay() {
        long d = delayMs;
        if (d > 0) {
            try {
                Thread.sleep(d);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    // RPCs that LEAVE this node
    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
            MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
        if (dropReplies) {
            return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {
                @Override
                public void start(ClientCall.Listener<RespT> responseListener, Metadata headers) {
                    // the request is really sent; we throw away whatever comes back
                    super.start(new ClientCall.Listener<RespT>() {
                        @Override
                        public void onClose(Status status, Metadata trailers) {
                            // DEADLINE_EXCEEDED, not UNAVAILABLE: the request DID arrive, so the outcome is unknown
                            responseListener.onClose(
                                    Status.DEADLINE_EXCEEDED.withDescription("reply dropped by fault injection"),
                                    new Metadata());
                        }
                    }, headers);
                }
            };
        }
        if (shouldDrop()) {
            return new DroppedCall<>();
        }
        return next.newCall(method, callOptions);
    }

    // RPCs that REACH this node
    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        if (shouldDrop()) {
            call.close(Status.UNAVAILABLE.withDescription("dropped by fault injection"), new Metadata());
            return new ServerCall.Listener<ReqT>() { };
        }
        sleepDelay();
        return next.startCall(call, headers);
    }

    // a call that fails at once with UNAVAILABLE, as if the network had lost the message
    private static class DroppedCall<ReqT, RespT> extends ClientCall<ReqT, RespT> {
        @Override
        public void start(Listener<RespT> responseListener, Metadata headers) {
            responseListener.onClose(Status.UNAVAILABLE.withDescription("dropped by fault injection"), new Metadata());
        }

        @Override
        public void request(int numMessages) { }

        @Override
        public void cancel(String message, Throwable cause) { }

        @Override
        public void halfClose() { }

        @Override
        public void sendMessage(ReqT message) { }
    }

    public void isolateFor(long seconds) {
        isolate();
        Thread timer = new Thread(() -> {
            try {
                Thread.sleep(seconds * 1000);
            } catch (InterruptedException e) {
                return;
            }
            off();
            System.out.println("[fault] isolation ended after " + seconds + " s");
        });
        timer.setDaemon(true);   // does not keep the program alive on CRASH or exit
        timer.start();
    }
}
