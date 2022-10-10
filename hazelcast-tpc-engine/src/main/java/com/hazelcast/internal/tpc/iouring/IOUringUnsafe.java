package com.hazelcast.internal.tpc.iouring;

import com.hazelcast.internal.tpc.AsyncFile;
import com.hazelcast.internal.tpc.Unsafe;
import com.hazelcast.internal.tpc.iobuffer.IOBufferAllocator;
import com.hazelcast.internal.tpc.iobuffer.NonConcurrentIOBufferAllocator;
import com.hazelcast.internal.tpc.util.LongObjectHashMap;

import static com.hazelcast.internal.tpc.util.OS.pageSize;

public class IOUringUnsafe extends Unsafe {

    private final IOUringEventloop ioUringEventloop;
    private long permanentHandlerIdGenerator = 0;
    private long temporaryHandlerIdGenerator = -1;

    final LongObjectHashMap<IOCompletionHandler> handlers = new LongObjectHashMap<>(4096);

    // this is not a very efficient allocator. It would be better to allocate a large chunk of
    // memory and then carve out smaller blocks. But for now it will do.
    private IOBufferAllocator storeIOBufferAllocator = new NonConcurrentIOBufferAllocator(4096, true, pageSize());

    public IOUringUnsafe(IOUringEventloop ioUringEventloop) {
        super(ioUringEventloop);
        this.ioUringEventloop = ioUringEventloop;
    }

    /**
     * Gets the next handler id for a permanent handler. A permanent handler stays registered after receiving
     * a completion event.
     *
     * @return the next handler id.
     */
    public long nextPermanentHandlerId() {
        return permanentHandlerIdGenerator++;
    }

    /**
     * Gets the next handler id for a temporary handler. A temporary handler is automatically removed after receiving
     * the completion event.
     *
     * @return the next handler id.
     */
    public long nextTemporaryHandlerId() {
        return temporaryHandlerIdGenerator--;
    }

    @Override
    public IOBufferAllocator fileIOBufferAllocator() {
        return storeIOBufferAllocator;
    }

    @Override
    public AsyncFile newAsyncFile(String path) {
        return new IOUringAsyncFile(path, ioUringEventloop);
    }
}
