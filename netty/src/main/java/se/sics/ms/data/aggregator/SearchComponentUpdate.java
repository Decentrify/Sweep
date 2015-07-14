package se.sics.ms.data.aggregator;


import se.sics.ms.aggregator.data.ComponentUpdate;
import se.sics.ms.types.PeerDescriptor;

/**
 * Search Component Update to the local aggregator.
 */
public class SearchComponentUpdate implements ComponentUpdate{
    
    
    private final int nodeId;
    private final int partitionId;
    private final int partitionDepth;
    private final long numberOfEntries;
    private final int componentOverlayId;
    
    public SearchComponentUpdate(PeerDescriptor desc, int componentOverlayId){
        
        this.nodeId = desc.getId();
        this.partitionId = desc.getOverlayId().getPartitionId();
        this.partitionDepth = desc.getOverlayId().getPartitionIdDepth();
        this.numberOfEntries = desc.getNumberOfIndexEntries();
        this.componentOverlayId = componentOverlayId;
    }
    
    public SearchComponentUpdate(int nodeId, int partitionId, int partitionDepth, long numberOfEntries, int componentOverlayId){

        this.nodeId = nodeId;
        this.partitionId = partitionId;
        this.partitionDepth = partitionDepth;
        this.numberOfEntries = numberOfEntries;
        this.componentOverlayId = componentOverlayId;
    }

    public int getNodeId() {
        return nodeId;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public int getPartitionDepth() {
        return partitionDepth;
    }

    public long getNumberOfEntries() {
        return numberOfEntries;
    }

    @Override
    public int getComponentOverlay() {
        return 0;
    }
}


