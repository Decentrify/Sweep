package se.kth.ms.partitionaware.core;
import se.sics.kompics.Init;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.network.KAddress;

/**
 * Initializer for the partition aware layer in the system.
 *  
 * Created by babbarshaer on 2015-06-27.
 */
public class PALInit extends Init<PartitionAwareLayer> {
    
    public final KAddress self;
    public final int bufferedHistorySize;
    public final Identifier overlayId;
    
    public PALInit(KAddress self, int bufferedHistorySize, Identifier overlayId){
        
        this.self = self;
        this.bufferedHistorySize = bufferedHistorySize;
        this.overlayId = overlayId;
    }
}
