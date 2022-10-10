package com.hazelcast.internal.tpc;

import com.hazelcast.internal.tpc.nio.NioEventloopBuilder;

import static java.util.concurrent.TimeUnit.SECONDS;

public class ScheduleMain {

    public static void main(String[] args) {
        NioEventloopBuilder eventloopBuilder = new NioEventloopBuilder();
        Eventloop eventloop = eventloopBuilder.create();
        eventloop.start();

        eventloop.offer(() -> eventloop.unsafe().scheduleWithFixedDelay(new Ping(), 1, 1, SECONDS));
    }

    private static class Ping implements Runnable {
        @Override
        public void run() {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            System.out.println(System.currentTimeMillis());
        }
    }
}
