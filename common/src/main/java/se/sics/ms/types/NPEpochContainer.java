package se.sics.ms.types;

/**
 * Container for the information exchanged during the 
 * process of healing of network partitioning.
 *  
 * The container in addition to basic epoch information contains 
 * information necessary for the seamless merging of the partitioned nodes.
 *
 * Created by babbarshaer on 2015-05-20.
 */
public class NPEpochContainer extends EpochContainer{
    
    public NPEpochContainer(long epochId, int leaderId, long numEntries) {
        super(epochId, leaderId, numEntries);
    }
    
    
}
