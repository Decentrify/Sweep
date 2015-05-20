package se.sics.ms.types;

/**
 * Simple Implementation of the shard container.
 * Formally Used when a node becomes the leader and takes further the 
 * epoch update cycle.
 *
 * Created by babbarshaer on 2015-05-20.
 */
public class BaseEpochContainer extends EpochContainer {
    
    public BaseEpochContainer(long epochId, int leaderId) {
        super(epochId, leaderId);
    }

    public BaseEpochContainer(long epochId, int leaderId, long numEntries) {
        super(epochId, leaderId, numEntries);
    }

    @Override
    public String toString() {
        return super.toString();
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }
}
