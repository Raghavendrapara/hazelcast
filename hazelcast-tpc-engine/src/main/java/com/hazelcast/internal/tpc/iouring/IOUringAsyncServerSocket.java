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

import com.hazelcast.internal.tpc.AsyncServerSocket;
import com.hazelcast.internal.tpc.AsyncSocket;
import com.hazelcast.internal.tpc.Eventloop;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.SocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static com.hazelcast.internal.tpc.iouring.IOUring.IORING_OP_ACCEPT;
import static com.hazelcast.internal.tpc.iouring.Linux.SOCK_CLOEXEC;
import static com.hazelcast.internal.tpc.iouring.Linux.SOCK_NONBLOCK;
import static com.hazelcast.internal.tpc.iouring.Linux.strerror;
import static com.hazelcast.internal.tpc.iouring.NativeSocket.AF_INET;
import static com.hazelcast.internal.tpc.util.Preconditions.checkNotNegative;
import static com.hazelcast.internal.tpc.util.Preconditions.checkNotNull;

/**
 * The io_uring implementation of the {@link AsyncServerSocket}.
 */
public final class IOUringAsyncServerSocket extends AsyncServerSocket {

    /**
     * Opens a new IOUringAsyncServerSocket.
     * <p/>
     * To prevent coupling, it is better to use the {@link Eventloop#openAsyncServerSocket()}.
     *
     * @param eventloop the IOUringEventloop this IOUringAsyncServerSocket belongs to.
     * @return the created IOUringAsyncServerSocket
     * @throws NullPointerException if eventloop is null.
     * @throws UncheckedIOException if there was a problem opening the socket.
     */
    public static IOUringAsyncServerSocket openTcpServerSocket(IOUringEventloop eventloop) {
        return new IOUringAsyncServerSocket(eventloop);
    }

    private final NativeSocket serverSocket;

    private final IOUringEventloop eventloop;
    private final AcceptMemory acceptMemory = new AcceptMemory();
    private SubmissionQueue sq;
    private Consumer<AsyncSocket> consumer;
    private long userdata_acceptHandler;
    private boolean bind = false;

    private IOUringAsyncServerSocket(IOUringEventloop eventloop) {
        this.serverSocket = NativeSocket.openTcpIpv4Socket();
        serverSocket.setBlocking(true);
        this.eventloop = checkNotNull(eventloop);

        if (!eventloop.registerCloseable(this)) {
            close();
            throw new IllegalStateException("EventLoop is not running");
        }

        // todo: return value not checked.
        eventloop.offer(() -> {
            // todo: on close we need to deregister
            sq = eventloop.sq;
            IOUringUnsafe unsafe = (IOUringUnsafe) eventloop.unsafe();
            this.userdata_acceptHandler = unsafe.nextPermanentHandlerId();
            unsafe.handlers.put(userdata_acceptHandler, new Handler_OP_ACCEPT());
        });
    }

    /**
     * Returns the underlying {@link NativeSocket}.
     *
     * @return the {@link NativeSocket}.
     */
    public NativeSocket serverSocket() {
        return serverSocket;
    }

    @Override
    public int getLocalPort() {
        if (!bind) {
            return -1;
        } else {
            return serverSocket.getLocalAddress().getPort();
        }
    }

    @Override
    public IOUringEventloop getEventloop() {
        return eventloop;
    }

    @Override
    protected SocketAddress getLocalAddress0() {
        if (!bind) {
            return null;
        } else {
            return serverSocket.getLocalAddress();
        }
    }

    @Override
    public boolean isReusePort() {
        try {
            return serverSocket.isReusePort();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void setReusePort(boolean reusePort) {
        try {
            serverSocket.setReusePort(reusePort);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public boolean isReuseAddress() {
        try {
            return serverSocket.isReuseAddress();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void setReuseAddress(boolean reuseAddress) {
        try {
            serverSocket.setReuseAddress(reuseAddress);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void setReceiveBufferSize(int size) {
        try {
            serverSocket.setReceiveBufferSize(size);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public int getReceiveBufferSize() {
        try {
            return serverSocket.getReceiveBufferSize();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    protected void close0() throws IOException {
        eventloop.deregisterCloseable(this);
        serverSocket.close();
    }

    @Override
    public void bind(SocketAddress localAddress, int backlog) {
        checkNotNull(localAddress, "localAddress");
        checkNotNegative(backlog, "backlog");

        try {
            boolean blocking = serverSocket.isBlocking();
            serverSocket.setBlocking(true);
            serverSocket.bind(localAddress);
            serverSocket.listen(backlog);
            serverSocket.setBlocking(blocking);
            bind = true;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public CompletableFuture<Void> accept(Consumer<AsyncSocket> consumer) {
        checkNotNull(consumer, "consumer");

        CompletableFuture future = new CompletableFuture();
        eventloop.execute(() -> {
            try {
                this.consumer = consumer;
                if (!sq_offer_OP_ACCEPT()) {
                    throw new IllegalStateException("Submission queue rejected the OP_ACCEPT");
                }
                if (logger.isInfoEnabled()) {
                    logger.info("ServerSocket listening at " + getLocalAddress());
                }
                future.complete(null);
            } catch (Exception e) {
                future.completeExceptionally(e);
            }
        });
        return future;
    }

    private boolean sq_offer_OP_ACCEPT() {
        return sq.offer(
                IORING_OP_ACCEPT,
                0,
                SOCK_NONBLOCK | SOCK_CLOEXEC,
                serverSocket.fd(),
                acceptMemory.memoryAddress,
                0,
                acceptMemory.lengthMemoryAddress,
                userdata_acceptHandler
        );
    }


    private class Handler_OP_ACCEPT implements IOCompletionHandler {

        @Override
        public void handle(int res, int flags, long userdata) {
            try {
                if (res < 0) {
                    throw new UncheckedIOException(new IOException(strerror(-res)));
                }

                SocketAddress address = NativeSocket.toInetSocketAddress(acceptMemory.memoryAddress, acceptMemory.lengthMemoryAddress);

                if (logger.isInfoEnabled()) {
                    logger.info(IOUringAsyncServerSocket.this + " new connected accepted: " + address);
                }

                // todo: ugly that AF_INET is hard configured.
                // We should use the address to determine the type
                NativeSocket socket = new NativeSocket(res, AF_INET);
                IOUringAsyncSocket asyncSocket = new IOUringAsyncSocket(socket);
                consumer.accept(asyncSocket);

                // we need to reregister for more accepts.
                sq_offer_OP_ACCEPT();
                //todo: return value
            } catch (Exception e) {
                close("Closing IOUringAsyncServerSocket due to exception", e);
            }
        }
    }

}
