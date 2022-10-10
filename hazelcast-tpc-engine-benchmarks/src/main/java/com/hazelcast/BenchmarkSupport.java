package com.hazelcast;

import com.hazelcast.internal.tpc.Eventloop;

import static java.util.concurrent.TimeUnit.SECONDS;

public class BenchmarkSupport {

    public static final int TERMINATION_TIMEOUT_SECONDS = 30;

    public static void terminate(Eventloop eventloop) {
        if (eventloop == null) {
            return;
        }

        eventloop.shutdown();
        try {
            if (!eventloop.awaitTermination(TERMINATION_TIMEOUT_SECONDS, SECONDS)) {
                throw new RuntimeException("Eventloop failed to terminate within timeout.");
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
