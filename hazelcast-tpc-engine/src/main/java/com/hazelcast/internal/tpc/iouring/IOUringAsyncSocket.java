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

import com.hazelcast.internal.tpc.AsyncSocket;
import com.hazelcast.internal.tpc.Eventloop;
import com.hazelcast.internal.tpc.iobuffer.IOBuffer;
import org.jctools.queues.MpmcArrayQueue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.internal.tpc.iouring.IOUring.IORING_OP_RECV;
import static com.hazelcast.internal.tpc.iouring.IOUring.IORING_OP_SEND;
import static com.hazelcast.internal.tpc.iouring.IOUring.IORING_OP_WRITEV;
import static com.hazelcast.internal.tpc.iouring.Linux.IOV_MAX;
import static com.hazelcast.internal.tpc.util.BufferUtil.addressOf;
import static com.hazelcast.internal.tpc.util.BufferUtil.compactOrClear;
import static com.hazelcast.internal.tpc.util.Preconditions.checkInstanceOf;


public final class IOUringAsyncSocket extends AsyncSocket {

    static {
        // Ensure JNI is initialized as soon as this class is loaded
        IOUringLibrary.ensureAvailable();
    }

    /**
     * Opens an IOUringAsyncSocket.
     * <p/>
     * To prevent coupling, it is better to use the {@link Eventloop#openTcpAsyncSocket()}.
     *
     * @return the opened IOUringAsyncSocket.
     */
    public static IOUringAsyncSocket openTcpSocket() {
        return new IOUringAsyncSocket();
    }

    private Thread eventloopThread;

    private Handler_OP_READ handler_OP_READ;
    private long userdata_OP_READ;

    private Handler_OP_WRITE handler_OP_WRITE;
    private long userdata_OP_WRITE;

    private Handler_OP_WRITEV handler_op_WRITEV;
    private long userdata_OP_WRITEV;

    private final NativeSocket socket;
    private IOUringEventloop eventloop;


    // ======================================================
    // For the reading side of the socket
    // ======================================================
    private ByteBuffer receiveBuff;
    private SubmissionQueue sq;

    // ======================================================
    // for the writing side of the socket.
    // ======================================================
    // concurrent state
    public AtomicReference<Thread> flushThread = new AtomicReference<>();
    public final MpmcArrayQueue<IOBuffer> unflushedBufs = new MpmcArrayQueue<>(4096);

    // isolated state.
    public final IOVector ioVector = new IOVector(IOV_MAX);
    private final EventloopTask eventloopTask = new EventloopTask();

    private IOUringAsyncSocket() {
        this.socket = NativeSocket.openTcpIpv4Socket();
        this.clientSide = true;
    }

    IOUringAsyncSocket(NativeSocket socket) {
        this.socket = socket;
        this.remoteAddress = socket.getRemoteAddress();
        this.localAddress = socket.getLocalAddress();
        this.clientSide = false;
    }

    @Override
    public IOUringEventloop eventloop() {
        return eventloop;
    }

    public NativeSocket socket() {
        return socket;
    }

    @Override
    public void setKeepAlive(boolean keepAlive) {
        try {
            socket.setKeepAlive(keepAlive);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public boolean isKeepAlive() {
        try {
            return socket.isKeepAlive();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public boolean isTcpNoDelay() {
        try {
            return socket.isTcpNoDelay();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void setTcpNoDelay(boolean tcpNoDelay) {
        try {
            socket.setTcpNoDelay(tcpNoDelay);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public int getReceiveBufferSize() {
        try {
            return socket.getReceiveBufferSize();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void setReceiveBufferSize(int size) {
        try {
            socket.setReceiveBufferSize(size);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public int getSendBufferSize() {
        try {
            return socket.getSendBufferSize();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void setSendBufferSize(int size) {
        try {
            socket.setSendBufferSize(size);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void setTcpKeepAliveTime(int keepAliveTime) {
        try {
            socket.setTcpKeepAliveTime(keepAliveTime);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public int getTcpKeepAliveTime() {
        try {
            return socket.getTcpKeepAliveTime();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void setTcpKeepaliveIntvl(int keepaliveIntvl) {
        try {
            socket.setTcpKeepaliveIntvl(keepaliveIntvl);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public int getTcpKeepaliveIntvl() {
        try {
            return socket.getTcpKeepaliveIntvl();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void setTcpKeepAliveProbes(int keepAliveProbes) {
        try {
            socket.setTcpKeepAliveProbes(keepAliveProbes);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public int getTcpKeepaliveProbes() {
        try {
            return socket.getTcpKeepaliveProbes();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void activate(Eventloop eventloop) {
        if (this.eventloop != null) {
            throw new IllegalStateException(this + " already has been activated");
        }

        if (readHandler == null) {
            throw new IllegalStateException("Can't activate " + this + ": readhandler isn't set");
        }

        this.eventloop = checkInstanceOf(IOUringEventloop.class, eventloop, "evenloop");
        this.eventloopThread = this.eventloop.eventloopThread();
        this.eventloop.offer(() -> {
            IOUringUnsafe unsafe = (IOUringUnsafe) this.eventloop.unsafe();

            handler_OP_READ = new Handler_OP_READ();
            userdata_OP_READ = unsafe.nextPermanentHandlerId();
            unsafe.handlers.put(userdata_OP_READ, handler_OP_READ);

            handler_OP_WRITE = new Handler_OP_WRITE();
            userdata_OP_WRITE = unsafe.nextPermanentHandlerId();
            unsafe.handlers.put(userdata_OP_WRITE, handler_OP_WRITE);

            handler_op_WRITEV = new Handler_OP_WRITEV();
            userdata_OP_WRITEV = unsafe.nextPermanentHandlerId();
            unsafe.handlers.put(userdata_OP_WRITEV, handler_op_WRITEV);

            // todo: deal with return value
            IOUringAsyncSocket.this.eventloop.registerCloseable(IOUringAsyncSocket.this);

            // todo: on closing of the socket we need to deregister the event handlers.

            sq = this.eventloop.sq;
            receiveBuff = ByteBuffer.allocateDirect(getReceiveBufferSize());

            if (!clientSide) {
                sq_addRead();
            }
        });
    }

    @Override
    public void flush() {
        Thread currentThread = Thread.currentThread();
        if (flushThread.compareAndSet(null, currentThread)) {
            if (currentThread == eventloopThread) {
                eventloop.localTaskQueue.add(eventloopTask);
            } else {
                eventloop.offer(eventloopTask);
            }
        }
    }

    private void resetFlushed() {
        if (!ioVector.isEmpty()) {
            eventloop.localTaskQueue.add(eventloopTask);
            return;
        }

        flushThread.set(null);

        if (!unflushedBufs.isEmpty()) {
            if (flushThread.compareAndSet(null, Thread.currentThread())) {
                eventloop.offer(eventloopTask);
            }
        }
    }

    @Override
    public boolean write(IOBuffer buf) {
        if (!buf.byteBuffer().isDirect()) {
            throw new IllegalArgumentException();
        }
        return unflushedBufs.add(buf);
    }

    @Override
    public boolean writeAll(Collection<IOBuffer> bufs) {
        return unflushedBufs.addAll(bufs);
    }

    @Override
    public boolean writeAndFlush(IOBuffer buf) {
        if (!buf.byteBuffer().isDirect()) {
            throw new IllegalArgumentException();
        }
        boolean result = unflushedBufs.add(buf);
        flush();
        return result;
    }

    @Override
    public boolean unsafeWriteAndFlush(IOBuffer buf) {
        Thread currentFlushThread = flushThread.get();
        Thread currentThread = Thread.currentThread();

        assert currentThread == eventloopThread;

        boolean result;
        if (currentFlushThread == null) {
            if (flushThread.compareAndSet(null, currentThread)) {
                eventloop.localTaskQueue.add(eventloopTask);
                if (ioVector.offer(buf)) {
                    result = true;
                } else {
                    result = unflushedBufs.offer(buf);
                }
            } else {
                result = unflushedBufs.offer(buf);
            }
        } else if (currentFlushThread == eventloopThread) {
            if (ioVector.offer(buf)) {
                result = true;
            } else {
                result = unflushedBufs.offer(buf);
            }
        } else {
            result = unflushedBufs.offer(buf);
            flush();
        }
        return result;
    }

    @Override
    protected void close0() throws IOException {
        //todo: also think about releasing the resources like IOBuffers

        if (eventloop != null) {
            eventloop.deregisterCloseable(this);
        }

        if (socket != null) {
            socket.close();
        }
    }

    // todo: boolean return
    private void sq_addRead() {
        int pos = receiveBuff.position();
        long address = addressOf(receiveBuff) + pos;
        int length = receiveBuff.remaining();
        if (length == 0) {
            throw new RuntimeException("Calling sq_addRead with 0 length for the read buffer");
        }

        // IORING_OP_RECV provides better performance than IORING_OP_READ
        // https://github.com/axboe/liburing/issues/536
        sq.offer(
                IORING_OP_RECV,     // op
                0,                  // flags
                0,                  // rw-flags
                socket.fd(),      // fd
                address,            // buffer address
                length,             // length
                0,                  // offset
                userdata_OP_READ    // userdata
        );
    }

    @Override
    public CompletableFuture<Void> connect(SocketAddress address) {
        if (logger.isFineEnabled()) {
            logger.fine("Connect to address:" + address);
        }
        if (eventloop == null) {
            throw new IllegalStateException("Can't connect, IOUringAsyncSocket isn't activated.");
        }

        CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            boolean oldBlocking = socket.isBlocking();
            socket.setBlocking(true);
            boolean connect = socket.connect(address);
            socket.setBlocking(false);
            if (connect) {
                this.remoteAddress = socket.getRemoteAddress();
                this.localAddress = socket.getLocalAddress();

                if (logger.isInfoEnabled()) {
                    logger.info("Connected from " + localAddress + "->" + remoteAddress);
                }

                eventloop.offer(() -> sq_addRead());

                future.complete(null);
            } else {
                future.completeExceptionally(new IOException("Could not connect to " + address));
            }
        } catch (Exception e) {
            logger.warning(e);
            future.completeExceptionally(e);
        }

        return future;
    }

    private class EventloopTask implements Runnable {

        @Override
        public void run() {
            try {
                if (flushThread.get() == null) {
                    throw new RuntimeException("Channel should be in flushed state");
                }

                ioVector.populate(unflushedBufs);

                if (ioVector.count() == 1) {
                    // There is just one item in the ioVecArray, so instead of doing a vectorized write, we do a regular write.
                    ByteBuffer buffer = ioVector.get(0).byteBuffer();
                    long bufferAddress = addressOf(buffer);
                    long address = bufferAddress + buffer.position();
                    int length = buffer.remaining();

                    // todo: return value
                    sq.offer(
                            IORING_OP_SEND,         // op
                            0,                       // flags
                            0,                       // rw-flags
                            socket.fd(),           // fd
                            address,                 // buffer address
                            length,                  // number of bytes to write.
                            0,                       // offset
                            userdata_OP_WRITE        // userdata
                    );
                } else {
                    long address = ioVector.addr();
                    int count = ioVector.count();

                    // todo: return value
                    sq.offer(
                            IORING_OP_WRITEV,       // op
                            0,                      // flags
                            0,                      // rw-flags
                            socket.fd(),          // fd
                            address,                // iov start address
                            count,                  // number of iov entries
                            0,                      // offset
                            userdata_OP_WRITEV      // userdata
                    );
                }
            } catch (Exception e) {
                close("Closing IOUringAsyncSocket due to exception", e);
            }
        }
    }

    private class Handler_OP_WRITEV implements IOCompletionHandler {
        @Override
        public void handle(int res, int flags, long userdata) {
            try {
                if (res < 0) {
                    throw new UncheckedIOException(new IOException("Socket writev failed. " + Linux.strerror(-res)));
                }

                //System.out.println(IOUringAsyncSocket.this + " written " + res);

                ioVector.compact(res);
                resetFlushed();
            } catch (Exception e) {
                close("Closing IOUringAsyncSocket due to exception", e);
            }
        }
    }

    private class Handler_OP_WRITE implements IOCompletionHandler {
        @Override
        public void handle(int res, int flags, long userdata) {
            try {
                if (res < 0) {
                    throw new UncheckedIOException(new IOException("Socket write failed" + Linux.strerror(-res)));
                }
                //System.out.println(IOUringAsyncSocket.this + " written " + res);

                ioVector.compact(res);
                resetFlushed();
            } catch (Exception e) {
                close("Closing IOUringAsyncSocket due to exception", e);
            }
        }
    }

    private class Handler_OP_READ implements IOCompletionHandler {
        @Override
        public void handle(int res, int flags, long userdata) {
            try {
                if (res < 0) {
                    throw new UncheckedIOException(new IOException("Socket read failed. " + Linux.strerror(-res)));
                }

                //System.out.println("Bytes read:" + res);

                int read = res;
                readEvents.inc();
                bytesRead.inc(read);

                // io_uring has written the new data into the byteBuffer, but the position we
                // need to manually update.
                receiveBuff.position(receiveBuff.position() + read);

                // prepare buffer for reading
                receiveBuff.flip();

                // offer the read data for processing
                readHandler.onRead(receiveBuff);

                // prepare buffer for writing.
                compactOrClear(receiveBuff);

                // signal that we want to read more data.
                sq_addRead();
            } catch (Exception e) {
                close("Closing IOUringAsyncSocket due to exception", e);
            }
        }
    }
}
