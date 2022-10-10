package com.hazelcast.internal.tpc.iouring;

import com.hazelcast.internal.tpc.Eventloop;
import com.hazelcast.internal.tpc.EventloopBuilder;
import com.hazelcast.internal.tpc.EventloopType;

import static com.hazelcast.internal.tpc.util.Preconditions.checkNotNegative;
import static com.hazelcast.internal.tpc.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.tpc.util.Preconditions.checkPositive;

/**
 * Contains the configuration for the {@link IOUringEventloop}.
 */
public class IOUringEventloopBuilder extends EventloopBuilder {
    private static final int DEFAULT_IOURING_SIZE = 8192;

    int setupFlags;
    int entries = DEFAULT_IOURING_SIZE;
    StorageDeviceRegistry deviceRegistry = new StorageDeviceRegistry();
    boolean registerRing;

    public IOUringEventloopBuilder() {
        super(EventloopType.IOURING);
    }

    @Override
    public Eventloop create() {
        return new IOUringEventloop(this);
    }

    /**
     * Configures if the file descriptor of the io_uring instance should be
     * registered. The purpose of registration it to speed up io_uring_enter.
     * <p/>
     * For more information see:
     * https://man7.org/linux/man-pages/man3/io_uring_register_ring_fd.3.html
     * <p/>
     * This is an ultra power feature and should probably not be used by anyone.
     * You can only have 16 io_uring instances with registered ring file
     * descriptor. If you create more, you will run into a 'Device or resource busy'
     * exception.
     *
     * @param registerRingFd
     */
    public void setRegisterRingFd(boolean registerRingFd){
        this.registerRing = registerRingFd;
    }

    /**
     * Sets the setup flags for the io_uring instance. See the IoUring.IORING_SETUP
     * constants.
     *
     * @param setupFlags the flags
     * @throws IllegalArgumentException if flags smaller than 0.
     */
    public void setSetupFlags(int setupFlags) {
        this.setupFlags = checkNotNegative(setupFlags, "setupFlags");
    }

    /**
     * Sets the number of entries for the io_uring instance.
     * <p/>
     * For more information see:
     * https://man7.org/linux/man-pages//man2/io_uring_enter.2.html
     *
     * @param entries the number of entries.
     * @throws IllegalArgumentException when entries smaller than 1.
     */
    public void setEntries(int entries) {
        this.entries = checkPositive(entries, "entries");
    }

    public void setStorageDeviceRegistry(StorageDeviceRegistry deviceRegistry) {
        this.deviceRegistry = checkNotNull(deviceRegistry);
    }
}
