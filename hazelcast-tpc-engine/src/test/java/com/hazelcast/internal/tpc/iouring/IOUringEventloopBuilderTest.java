package com.hazelcast.internal.tpc.iouring;

import com.hazelcast.internal.tpc.EventloopBuilder;
import com.hazelcast.internal.tpc.EventloopBuilderTest;

public class IOUringEventloopBuilderTest extends EventloopBuilderTest {

    @Override
    public EventloopBuilder newBuilder() {
        return new IOUringEventloopBuilder();
    }
}
