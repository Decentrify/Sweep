package se.sics.ms.gradient.events;

import se.sics.kompics.Event;

import java.util.UUID;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 9/6/13
 * Time: 8:18 PM
 */
public class NumberOfPartitions extends Event {

    private final UUID timeoutId;
    private final int numberOfShards;

    public NumberOfPartitions(UUID timeoutId, int numberOfShards) {

        this.timeoutId = timeoutId;
        this.numberOfShards = numberOfShards;
    }

    public UUID getTimeoutId() {
        return timeoutId;
    }

    public int getNumberOfShards() {
        return numberOfShards;
    }
}
