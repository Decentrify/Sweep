package se.sics.ms.types;

/**
 * Epoch Container for the Sharding Information.
 * Used for containing information that will be exchanged during
 * the sharding phase.
 *  
 * Created by babbarshaer on 2015-05-20.
 */
public class ShardEpochContainer extends EpochContainer{
    
    
    public ShardEpochContainer(long epochId, int leaderId) {
        super(epochId, leaderId);
    }

    public ShardEpochContainer(long epochId, int leaderId, long numEntries) {
        super(epochId, leaderId, numEntries);
    }
    
    
    
    
    
    
    
}
