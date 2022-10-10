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

package com.hazelcast.internal.alto;

import com.hazelcast.internal.tpc.Scheduler;
import com.hazelcast.internal.tpc.util.CircularQueue;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.internal.tpc.Eventloop;
import com.hazelcast.internal.tpc.iobuffer.IOBuffer;
import com.hazelcast.internal.tpc.iobuffer.IOBufferAllocator;

import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;
import static com.hazelcast.internal.alto.FrameCodec.FLAG_OP_RESPONSE_CONTROL;
import static com.hazelcast.internal.alto.FrameCodec.OFFSET_PARTITION_ID;
import static com.hazelcast.internal.alto.FrameCodec.OFFSET_REQ_CALL_ID;
import static com.hazelcast.internal.alto.FrameCodec.OFFSET_REQ_PAYLOAD;
import static com.hazelcast.internal.alto.FrameCodec.OFFSET_RES_PAYLOAD;
import static com.hazelcast.internal.alto.FrameCodec.RESPONSE_TYPE_EXCEPTION;
import static com.hazelcast.internal.alto.FrameCodec.RESPONSE_TYPE_OVERLOAD;
import static com.hazelcast.internal.alto.Op.BLOCKED;
import static com.hazelcast.internal.alto.Op.COMPLETED;
import static com.hazelcast.internal.alto.Op.EXCEPTION;

/**
 * todo: add control on number of requests of single socket.
 * overload can happen at 2 levels
 * 1) a single asyncsocket exceeding the maximum number of requests
 * 2) a single asyncsocket exceeded the maximum number of request of the event loop
 * <p>
 * In the first case we want to slow down the reader and signal sender.
 * In the second case want to slow down all the readers and signal all senders.
 * <p>
 * The problem with slowing down senders is that sockets can send
 * either requests or responses. We want to slow down the rate of sending
 * requests, but we don't want to slow down the rate of responses or
 * other data.
 */
public final class OpScheduler implements Scheduler {

    private final SwCounter scheduled = newSwCounter();
    private final SwCounter ticks = newSwCounter();
    private final SwCounter completed = newSwCounter();
    private final SwCounter exceptions = newSwCounter();
    private final CircularQueue<Op> runQueue;
    private final int batchSize;
    private final IOBufferAllocator localResponseAllocator;
    private final IOBufferAllocator remoteResponseAllocator;
    private final OpAllocator opAllocator;
    private Eventloop eventloop;
    private ResponseHandler responseHandler;

    public OpScheduler(int capacity,
                       int batchSize,
                       Managers managers,
                       IOBufferAllocator localResponseAllocator,
                       IOBufferAllocator remoteResponseAllocator,
                       ResponseHandler responseHandler) {
        this.runQueue = new CircularQueue<>(capacity);
        this.batchSize = batchSize;
        this.localResponseAllocator = localResponseAllocator;
        this.remoteResponseAllocator = remoteResponseAllocator;
        this.responseHandler = responseHandler;
        this.opAllocator = new OpAllocator(this, managers);
    }

    public long getScheduled(){
        return scheduled.get();
    }

    @Override
    public void init(Eventloop eventloop) {
        this.eventloop = eventloop;
    }

    public Eventloop getEventloop() {
        return eventloop;
    }

    @Override
    public void schedule(IOBuffer request) {
        Op op = opAllocator.allocate(request);
//        if (op.partitionId >= 0) {
//            Actor partitionActor = partitionActors[op.partitionId];
//            if (partitionActor == null) {
//                partitionActor = new PartitionActor();
//                partitionActors[op.partitionId] = partitionActor;
//            }
//            partitionActor.handle().send(op);
//        } else {
//
//        }

        op.partitionId = request.getInt(OFFSET_PARTITION_ID);
        op.callId = request.getLong(OFFSET_REQ_CALL_ID);
        op.response = request.socket != null
                ? remoteResponseAllocator.allocate(OFFSET_RES_PAYLOAD)
                : localResponseAllocator.allocate(OFFSET_RES_PAYLOAD);
        op.request = request;
        op.request.position(OFFSET_REQ_PAYLOAD);
        schedule(op);
    }

    public void schedule(Op op) {
        scheduled.inc();

        if (runQueue.offer(op)) {
            runSingle();
        } else {
            IOBuffer response = op.response;
            FrameCodec.writeResponseHeader(response, op.partitionId, op.callId, FLAG_OP_RESPONSE_CONTROL);
            response.writeInt(RESPONSE_TYPE_OVERLOAD);
            FrameCodec.constructComplete(response);
            sendResponse(op);
        }
    }

    @Override
    public boolean tick() {
        ticks.inc();

        for (int k = 0; k < batchSize - 1; k++) {
            if (!runSingle()) {
                return false;
            }
        }

        return runSingle();
    }

    public boolean runSingle() {
        Op op = runQueue.poll();
        if (op == null) {
            return false;
        }

        try {
            int runCode;
            Exception exception = null;
            try {
                runCode = op.run();
            } catch (Exception e) {
                exception = e;
                runCode = EXCEPTION;
            }

            switch (runCode) {
                case COMPLETED:
                    completed.inc();
                    sendResponse(op);
                    break;
                case BLOCKED:
                    break;
                case EXCEPTION:
                    exceptions.inc();
                    op.response.clear();
                    FrameCodec.writeResponseHeader(op.response, op.partitionId, op.callId, FLAG_OP_RESPONSE_CONTROL);
                    op.response.writeInt(RESPONSE_TYPE_EXCEPTION);
                    op.response.writeString(exception.getMessage());
                    FrameCodec.constructComplete(op.response);
                    sendResponse(op);
                    break;
                default:
                    throw new RuntimeException();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return !runQueue.isEmpty();
    }

    private void sendResponse(Op op) {
        if (op.request.socket != null) {
            op.request.socket.unsafeWriteAndFlush(op.response);
        } else {
            responseHandler.accept(op.response);
        }
        op.request.release();
        op.release();
    }
}
