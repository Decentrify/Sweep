package se.kth.ms.partitionaware.core;
import se.sics.kompics.Init;
import se.sics.p2ptoolbox.util.network.impl.BasicAddress;

/**
 * Initializer for the partition aware layer in the system.
 *  
 * Created by babbarshaer on 2015-06-27.
 */
public class PALInit extends Init<PartitionAwareLayer> {
    
    public BasicAddress selfBase;
    public int bufferedHistorySize;
    public int overlayId;
    
    public PALInit(BasicAddress selfBase, int bufferedHistorySize, int overlayId){
        
        this.selfBase = selfBase;
        this.bufferedHistorySize = bufferedHistorySize;
        this.overlayId = overlayId;
    }
    
}