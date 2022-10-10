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
import com.hazelcast.internal.tpc.Scheduler;
import com.hazelcast.internal.tpc.Unsafe;
import com.hazelcast.internal.tpc.util.LongObjectHashMap;
import com.hazelcast.internal.tpc.util.NanoClock;
import com.hazelcast.internal.tpc.util.UnsafeLocator;
import org.jctools.queues.MpmcArrayQueue;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.internal.tpc.Eventloop.State.RUNNING;
import static com.hazelcast.internal.tpc.iouring.IOUring.IORING_OP_READ;
import static com.hazelcast.internal.tpc.iouring.IOUring.IORING_OP_TIMEOUT;
import static com.hazelcast.internal.tpc.util.BitUtil.SIZEOF_LONG;
import static com.hazelcast.internal.tpc.util.CloseUtil.closeAllQuietly;
import static com.hazelcast.internal.tpc.util.CloseUtil.closeQuietly;
import static com.hazelcast.internal.tpc.util.Preconditions.checkNotNull;

/**
 * io_uring implementation of the {@link Eventloop}.
 *
 * <p>
 * Good read:
 * https://unixism.net/2020/04/io-uring-by-example-part-3-a-web-server-with-io-uring/
 * <p>
 * Another example (blocking socket)
 * https://github.com/ddeka0/AsyncIO/blob/master/src/asyncServer.cpp
 * <p>
 * no syscalls:
 * https://wjwh.eu/posts/2021-10-01-no-syscall-server-iouring.html
 */
public class IOUringEventloop extends Eventloop {

    private final static sun.misc.Unsafe UNSAFE = UnsafeLocator.UNSAFE;

    private  IOUring uring;
    private final IOUringEventloopBuilder builder;
    private  CompletionQueue cq;
    //todo: Litter; we need to come up with better solution.
    protected final Set<AutoCloseable> closeables = new CopyOnWriteArraySet<>();

    private final long timeoutSpecAddr = UNSAFE.allocateMemory(Linux.SIZEOF_KERNEL_TIMESPEC);
    private long userdata_timeout;

    private final EventFd eventfd = new EventFd();
    private final long eventFdReadBuf = UNSAFE.allocateMemory(SIZEOF_LONG);
    private long userdata_eventRead;

    SubmissionQueue sq;
    protected final StorageDeviceRegistry storageScheduler;
    private EventloopHandler eventLoopHandler;

    public IOUringEventloop() {
        this(new IOUringEventloopBuilder());
    }

    public IOUringEventloop(IOUringEventloopBuilder builder) {
        super(builder);
        this.builder = builder;

            this.storageScheduler = builder.deviceRegistry;
    }

    /**
     * Registers an AutoCloseable on this Eventloop.
     * <p>
     * Registered closeable are automatically closed when the eventloop closes.
     * Some examples: AsyncSocket and AsyncServerSocket.
     * <p>
     * If the Eventloop isn't in the running state, false is returned.
     * <p>
     * This method is thread-safe.
     *
     * @param closeable the AutoCloseable to register
     * @return true if the closeable was successfully register, false otherwise.
     * @throws NullPointerException if closeable is null.
     */
    public boolean registerCloseable(AutoCloseable closeable) {
        checkNotNull(closeable, "closeable");

        if (state != RUNNING) {
            return false;
        }

        closeables.add(closeable);

        if (state != RUNNING) {
            closeables.remove(closeable);
            return false;
        }

        return true;
    }

    /**
     * Deregisters an AutoCloseable from this Eventloop.
     * <p>
     * This method is thread-safe.
     * <p>
     * This method can be called no matter the state of the Eventloop.
     *
     * @param closeable the AutoCloseable to deregister.
     */
    public void deregisterCloseable(AutoCloseable closeable) {
        closeables.remove(checkNotNull(closeable, "closeable"));
    }

    @Override
    public AsyncServerSocket openTcpAsyncServerSocket() {
        return IOUringAsyncServerSocket.openTcpServerSocket(this);
    }

    @Override
    public AsyncSocket openTcpAsyncSocket() {
        return IOUringAsyncSocket.openTcpSocket();
    }

    @Override
    protected Unsafe createUnsafe() {
        return new IOUringUnsafe(this);
    }

    @Override
    protected void beforeEventloop() {
        // The uring instance needs to be created on the eventloop thread.
        // This is required for some of the setup flags.
        this.uring = new IOUring(builder.entries, builder.setupFlags);
        if (builder.registerRing) {
            this.uring.registerRingFd();
        }
        this.sq = uring.getSubmissionQueue();
        this.cq = uring.getCompletionQueue();

        this.eventLoopHandler = new EventloopHandler();
        storageScheduler.init(this);
        IOUringUnsafe unsafe = (IOUringUnsafe) unsafe();
        this.userdata_eventRead = unsafe.nextPermanentHandlerId();
        this.userdata_timeout = unsafe.nextPermanentHandlerId();
        unsafe.handlers.put(userdata_eventRead, new EventFdCompletionHandler());
        unsafe.handlers.put(userdata_timeout, new TimeoutCompletionHandler());
    }

    @Override
    protected void afterEventloop() {
        closeQuietly(uring);
        closeQuietly(eventfd);
        closeAllQuietly(closeables);
        closeables.clear();

        if (timeoutSpecAddr != 0) {
            UNSAFE.freeMemory(timeoutSpecAddr);
        }

        if (eventFdReadBuf != 0) {
            UNSAFE.freeMemory(eventFdReadBuf);
        }
    }

    @Override
    public void wakeup() {
        if (spin || Thread.currentThread() == eventloopThread) {
            return;
        }

        if (wakeupNeeded.get() && wakeupNeeded.compareAndSet(true, false)) {
            eventfd.write(1L);
        }
    }

    @Override
    protected void eventLoop() {
        final NanoClock nanoClock = unsafe.nanoClock();
        final EventloopHandler eventLoopHandler = this.eventLoopHandler;
        final AtomicBoolean wakeupNeeded = this.wakeupNeeded;
        final CompletionQueue cq = this.cq;
        final boolean spin = this.spin;
        final SubmissionQueue sq = this.sq;
        final MpmcArrayQueue concurrentTaskQueue = this.concurrentTaskQueue;
        final Scheduler scheduler = this.scheduler;

        sq_offerEventFdRead();

        boolean moreWork = false;
        do {
            if (cq.hasCompletions()) {
                // todo: do we want to control number of events being processed.
                cq.process(eventLoopHandler);
            } else {
                if (spin || moreWork) {
                    sq.submit();
                } else {
                    wakeupNeeded.set(true);
                    if (concurrentTaskQueue.isEmpty()) {
                        if (earliestDeadlineNanos != -1) {
                            long timeoutNanos = earliestDeadlineNanos - nanoClock.nanoTime();
                            if (timeoutNanos > 0) {
                                sq_offerTimeout(timeoutNanos);
                                sq.submitAndWait();
                            } else {
                                sq.submit();
                            }
                        } else {
                            sq.submitAndWait();
                        }
                    } else {
                        sq.submit();
                    }
                    wakeupNeeded.set(false);
                }
            }

            // what are the queues that are available for processing
            // 1: completion events
            // 2: concurrent task queue
            // 3: timed task queue
            // 4: local task queue
            // 5: scheduler task queue

            moreWork = runConcurrentTasks();
            moreWork |= scheduler.tick();
            moreWork |= runScheduledTasks();
            moreWork |= runLocalTasks();
        } while (state == State.RUNNING);
    }

    // todo: I'm questioning of this is not going to lead to problems. Can it happen that
    // multiple timeout requests are offered? So one timeout request is scheduled while another command is
    // already in the pipeline. Then the thread waits, and this earlier command completes while the later
    // timeout command is still scheduled. If another timeout is scheduled, then you have 2 timeouts in the
    // uring and both share the same timeoutSpecAddr.
    private void sq_offerTimeout(long timeoutNanos) {
        if (timeoutNanos <= 0) {
            UNSAFE.putLong(timeoutSpecAddr, 0);
            UNSAFE.putLong(timeoutSpecAddr + SIZEOF_LONG, 0);
        } else {
            long seconds = timeoutNanos / 1_000_000_000;
            UNSAFE.putLong(timeoutSpecAddr, seconds);
            UNSAFE.putLong(timeoutSpecAddr + SIZEOF_LONG, timeoutNanos - seconds * 1_000_000_000);
        }

        // todo: return value isn't checked
        sq.offer(IORING_OP_TIMEOUT,
                0,
                0,
                -1,
                timeoutSpecAddr,
                1,
                0,
                userdata_timeout);
    }

    private void sq_offerEventFdRead() {
        // todo: we are not checking return value.
        sq.offer(IORING_OP_READ,
                0,
                0,
                eventfd.fd(),
                eventFdReadBuf,
                SIZEOF_LONG,
                0,
                userdata_eventRead);
    }

    private class EventloopHandler implements IOCompletionHandler {
        final LongObjectHashMap<IOCompletionHandler> handlers = ((IOUringUnsafe)unsafe()).handlers;

        @Override
        public void handle(int res, int flags, long userdata) {
            // Temporary handlers have a userdata smaller than 0 and need to be removed
            // on completion.
            // Permanent handlers have a userdata equal or larger than 0 and should not
            // be removed on completion.
            IOCompletionHandler h = userdata >= 0
                    ? handlers.get(userdata)
                    : handlers.remove(userdata);

            if (h == null) {
                logger.warning("no handler found for: " + userdata);
            } else {
                h.handle(res, flags, userdata);
            }
        }
    }

    private class EventFdCompletionHandler implements IOCompletionHandler {
        @Override
        public void handle(int res, int flags, long userdata) {
            sq_offerEventFdRead();
        }
    }

    private class TimeoutCompletionHandler implements IOCompletionHandler {
        @Override
        public void handle(int res, int flags, long userdata) {
        }
    }

}

