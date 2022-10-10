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

package com.hazelcast.table.impl;

import com.hazelcast.internal.alto.AltoRuntime;
import com.hazelcast.internal.util.collection.Long2ObjectHashMap;
import com.hazelcast.internal.tpc.SyncSocket;
import com.hazelcast.internal.tpc.iobuffer.IOBuffer;
import com.hazelcast.internal.tpc.iobuffer.IOBufferAllocator;
import com.hazelcast.internal.alto.FrameCodec;
import com.hazelcast.table.Pipeline;

import java.util.List;

import static com.hazelcast.internal.alto.OpCodes.NOOP;


// todo: we don't need a IOBuffer for all the requests. We should just add to an existing IOBuffer.
public final class PipelineImpl implements Pipeline {

    private final AltoRuntime altoRuntime;
    private final IOBufferAllocator requestAllocator;
    private final Long2ObjectHashMap longToObjectHashMap = new Long2ObjectHashMap();
    private int partitionId = -1;
    private SyncSocket syncSocket;

    public PipelineImpl(AltoRuntime altoRuntime, IOBufferAllocator requestAllocator) {
        this.altoRuntime = altoRuntime;
        this.requestAllocator = requestAllocator;
    }

    public void noop(int partitionId) {
        if (partitionId < 0) {
            throw new IllegalArgumentException("PartitionId can't be smaller than 0");
        }

        if (this.partitionId == -1) {
            this.partitionId = partitionId;
        } else if (partitionId != this.partitionId) {
            throw new RuntimeException("Cross partition request detected; expected " + this.partitionId + " found: " + partitionId);
        }

        IOBuffer request = requestAllocator.allocate(32);
        FrameCodec.writeRequestHeader(request, partitionId, NOOP);
        FrameCodec.constructComplete(request);


        //altoRuntime.invokeOnPartition();
    }

    @Override
    public void execute() {
        //altoRuntime.invokeOnPartition(this);
    }

    public void await(){
//        for(Future<IOBuffer> f: futures){
//            try {
//                IOBuffer buf = f.get(altoRuntime.getRequestTimeoutMs(), MILLISECONDS);
//                buf.release();
//            } catch (Exception e) {
//                throw new RuntimeException(e);
//            }
//        }
    }

    public int getPartitionId() {
        return partitionId;
    }

    public List<IOBuffer> getRequests() {
        return null;//requests;
    }
}
