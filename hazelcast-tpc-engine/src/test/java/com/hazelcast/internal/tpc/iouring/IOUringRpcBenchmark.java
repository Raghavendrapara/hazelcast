/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.internal.tpc.iouring;


import com.hazelcast.internal.tpc.ReadHandler;
import com.hazelcast.internal.tpc.iobuffer.IOBuffer;
import com.hazelcast.internal.tpc.iobuffer.IOBufferAllocator;
import com.hazelcast.internal.tpc.iobuffer.NonConcurrentIOBufferAllocator;
import com.hazelcast.internal.util.ThreadAffinity;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.internal.tpc.TpcTestSupport.constructComplete;
import static com.hazelcast.internal.tpc.util.BitUtil.SIZEOF_INT;
import static com.hazelcast.internal.tpc.util.BitUtil.SIZEOF_LONG;

/**
 * A very trivial benchmark to measure the throughput we can get with a RPC system.
 * <p>
 * JMH would be better; but for now this will give some insights.
 */
public class IOUringRpcBenchmark {

    public static int serverCpu = -1;
    public static int clientCpu = -1;
    public static boolean spin = true;
    public static int requestTotal = 1000 * 1000;
    public static int concurrency = 10;

    public static void main(String[] args) throws InterruptedException {
        SocketAddress serverAddress = new InetSocketAddress("127.0.0.1", 5000);

        IOUringAsyncServerSocket socket = newServer(serverAddress);

        CountDownLatch latch = new CountDownLatch(concurrency);

        IOUringAsyncSocket clientSocket = newClient(serverAddress, latch);

        System.out.println("Starting");

        long startMs = System.currentTimeMillis();

        for (int k = 0; k < concurrency; k++) {
            IOBuffer buf = new IOBuffer(128, true);
            buf.writeInt(-1);
            buf.writeLong(requestTotal / concurrency);
            constructComplete(buf);
            clientSocket.write(buf);
        }
        clientSocket.flush();

        latch.await();

        long duration = System.currentTimeMillis() - startMs;
        float throughput = requestTotal * 1000f / duration;
        System.out.println("Throughput:" + throughput);
        System.exit(0);
    }

    private static IOUringAsyncSocket newClient(SocketAddress serverAddress, CountDownLatch latch) {
        IOUringEventloopBuilder config = new IOUringEventloopBuilder();
        if (clientCpu >= 0) {
            config.setThreadAffinity(new ThreadAffinity("" + clientCpu));
        }
        config.setSpin(spin);
        IOUringEventloop clientEventLoop = new IOUringEventloop(config);
        clientEventLoop.start();

        IOUringAsyncSocket clientSocket = IOUringAsyncSocket.openTcpSocket();
        clientSocket.setTcpNoDelay(true);
        clientSocket.setReadHandler(new ReadHandler() {
            private final IOBufferAllocator responseAllocator = new NonConcurrentIOBufferAllocator(8, true);

            @Override
            public void onRead(ByteBuffer receiveBuff) {
                for (; ; ) {
//                    try {
//                        Thread.sleep(1000);
//                    } catch (InterruptedException e) {
//                        throw new RuntimeException(e);
//                    }

                    if (receiveBuff.remaining() < SIZEOF_INT + SIZEOF_LONG) {
                        return;
                    }

                    int size = receiveBuff.getInt();

                    if (size == 0) {
                        throw new RuntimeException();
                    }

                    long l = receiveBuff.getLong();

                    if (l % 100000 == 0) {
                        System.out.println("at:" + l);
                    }
                    if (l == 0) {
                        latch.countDown();
                    } else {
                        IOBuffer buf = responseAllocator.allocate(8);
                        buf.writeInt(-1);
                        buf.writeLong(l);
                        constructComplete(buf);
                        socket.unsafeWriteAndFlush(buf);
                    }
                }
            }
        });
        clientSocket.activate(clientEventLoop);
        clientSocket.connect(serverAddress).join();
        return clientSocket;
    }

    private static IOUringAsyncServerSocket newServer(SocketAddress serverAddress) {
        IOUringEventloopBuilder config = new IOUringEventloopBuilder();
        config.setSpin(spin);
        if (serverCpu >= 0) {
            config.setThreadAffinity(new ThreadAffinity("" + serverCpu));
        }
        IOUringEventloop serverEventloop = new IOUringEventloop(config);
        serverEventloop.start();

        IOUringAsyncServerSocket serverSocket = IOUringAsyncServerSocket.openTcpServerSocket(serverEventloop);
        serverSocket.setReusePort(true);
        serverSocket.bind(serverAddress);
        serverSocket.accept(socket -> {
            socket.setTcpNoDelay(true);
            socket.setReadHandler(new ReadHandler() {
                private final IOBufferAllocator responseAllocator = new NonConcurrentIOBufferAllocator(8, true);

                @Override
                public void onRead(ByteBuffer receiveBuff) {
//                    try {
//                        Thread.sleep(1000);
//                    } catch (InterruptedException e) {
//                        throw new RuntimeException(e);
//                    }

                    for (; ; ) {
                        if (receiveBuff.remaining() < SIZEOF_INT + SIZEOF_LONG) {
                            return;
                        }
                        int size = receiveBuff.getInt();
                        long l = receiveBuff.getLong();

                        if (size == 0) {
                            throw new RuntimeException();
                        }

                        IOBuffer buf = responseAllocator.allocate(8);
                        buf.writeInt(-1);
                        buf.writeLong(l - 1);
                        constructComplete(buf);
                        socket.unsafeWriteAndFlush(buf);
                    }
                }
            });
            socket.activate(serverEventloop);
        });

        return serverSocket;
    }
}
