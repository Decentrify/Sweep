package se.sics.ms.messages;

import se.sics.gvod.net.VodAddress;
import se.sics.gvod.timer.TimeoutId;
import se.sics.kompics.Event;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 8/20/13
 * Time: 11:38 AM
 */
public class PartitionMessage extends Event {
    private final TimeoutId requestId;
    private final long medianId;
    private final VodAddress.PartitioningType partitionsNumber;


    public PartitionMessage(TimeoutId requestId, long medianId, VodAddress.PartitioningType partitionsNumber) {
        this.requestId = requestId;
        this.medianId = medianId;
        this.partitionsNumber = partitionsNumber;
    }

    public TimeoutId getRequestId() {
        return requestId;
    }

    public long getMedianId() {
        return medianId;
    }

    public VodAddress.PartitioningType getPartitionsNumber() {
        return partitionsNumber;
    }
}
