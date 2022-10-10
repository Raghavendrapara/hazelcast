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

package com.hazelcast.internal.tpc.nio;


import com.hazelcast.internal.tpc.ReadHandler;
import com.hazelcast.internal.tpc.iobuffer.IOBuffer;
import com.hazelcast.internal.tpc.iobuffer.IOBufferAllocator;
import com.hazelcast.internal.tpc.iobuffer.NonConcurrentIOBufferAllocator;

import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;

import static com.hazelcast.internal.tpc.TpcTestSupport.constructComplete;
import static com.hazelcast.internal.tpc.util.BitUtil.SIZEOF_INT;
import static com.hazelcast.internal.tpc.util.CloseUtil.closeQuietly;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class NioSyncSocket_IntegrationTest {

    private NioEventloop eventloop;
    private NioAsyncServerSocket serverSocket;
    private NioSyncSocket clientSocket;

    @Before
    public void setup() {
        eventloop = new NioEventloop();
        eventloop.start();
    }

    public void after() throws InterruptedException {
        closeQuietly(clientSocket);
        closeQuietly(serverSocket);


        if (eventloop != null) {
            eventloop.shutdown();
            assertTrue(eventloop.awaitTermination(5, SECONDS));
        }
    }

    @Test
    public void test() {
        SocketAddress serverAddress = new InetSocketAddress("127.0.0.1", 6000);

        createServer(serverAddress);

        createClient(serverAddress);

        for (int k = 0; k < 1000; k++) {
            System.out.println("at: " + k);
            IOBuffer request = new IOBuffer(128, true);
            request.writeInt(-1);
            constructComplete(request);
            clientSocket.writeAndFlush(request);

            IOBuffer response = clientSocket.read();
            assertNotNull(response);
        }
    }

    private void createClient(SocketAddress serverAddress) {
        clientSocket = NioSyncSocket.open();
        clientSocket.tcpNoDelay(true);
        clientSocket.readHandler(new NioSyncReadHandler() {
            private final IOBufferAllocator responseAllocator = new NonConcurrentIOBufferAllocator(8, true);

            @Override
            public void onRead(ByteBuffer receiveBuffer) {

            }

            @Override
            public IOBuffer decode(ByteBuffer buffer) {
                if (buffer.remaining() < SIZEOF_INT) {
                    return null;
                }

                int size = buffer.getInt();
                IOBuffer buf = responseAllocator.allocate(8);
                buf.writeInt(-1);
                constructComplete(buf);
                return buf;
            }
        });
        clientSocket.connect(serverAddress);
    }

    private void createServer(SocketAddress serverAddress) {
        serverSocket = NioAsyncServerSocket.openTcpServerSocket(eventloop);
        serverSocket.setReuseAddress(true);
        serverSocket.bind(serverAddress);
        serverSocket.accept(socket -> {
            socket.setTcpNoDelay(true);
            socket.setReadHandler(new ReadHandler() {
                private final IOBufferAllocator responseAllocator = new NonConcurrentIOBufferAllocator(8, true);

                @Override
                public void onRead(ByteBuffer receiveBuffer) {
                    for (; ; ) {
                        if (receiveBuffer.remaining() < SIZEOF_INT) {
                            return;
                        }
                        int size = receiveBuffer.getInt();

                        IOBuffer buf = responseAllocator.allocate(8);
                        buf.writeInt(-1);
                        buf.flip();
                        socket.unsafeWriteAndFlush(buf);
                    }
                }
            });
            socket.activate(eventloop);
        });
    }
}
