package se.sics.ms.gradient.events;

import se.sics.gvod.timer.TimeoutId;
import se.sics.kompics.Event;
import se.sics.ms.types.SearchPattern;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 9/6/13
 * Time: 8:18 PM
 */
public class NumberOfPartitions extends Event {
    private final TimeoutId timeoutId;
    private final int numberOfPartitions;

    public NumberOfPartitions(TimeoutId timeoutId, int numberOfPartitions) {
        this.timeoutId = timeoutId;
        this.numberOfPartitions = numberOfPartitions;
    }

    public TimeoutId getTimeoutId() {
        return timeoutId;
    }

    public int getNumberOfPartitions() {
        return numberOfPartitions;
    }
}
