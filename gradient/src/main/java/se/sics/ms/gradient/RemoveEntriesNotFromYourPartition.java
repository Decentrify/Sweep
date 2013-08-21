package se.sics.ms.gradient;

import se.sics.kompics.Event;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 8/21/13
 * Time: 10:13 AM
 */
public class RemoveEntriesNotFromYourPartition extends Event{
    private final boolean partition;
    private final long middleId;

    public RemoveEntriesNotFromYourPartition(boolean partition, long middleId) {
        this.partition = partition;
        this.middleId = middleId;
    }

    public boolean isPartition() {
        return partition;
    }

    public long getMiddleId() {
        return middleId;
    }
}
