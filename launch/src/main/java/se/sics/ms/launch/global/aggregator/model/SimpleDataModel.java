package se.sics.ms.launch.global.aggregator.model;

/**
 * Based on the display requirement, these models can be defined.
 * The data would be populated from the aggregated data from the main search application.
 * Created by babbarshaer on 2015-03-20.
 */
public class SimpleDataModel {
    
    private int nodeId;
    private int partitionId;
    private int partitionDepth;
    private long numberOfEntries;
    private boolean isLeader;


    public SimpleDataModel(int nodeId, int partitionId, int partitionDepth, long numberOfEntries, boolean isLeader) {
        
        this.nodeId = nodeId;
        this.partitionId = partitionId;
        this.partitionDepth = partitionDepth;
        this.numberOfEntries = numberOfEntries;
        this.isLeader = isLeader;
    }
    
    public SimpleDataModel(int nodeId){
        this.nodeId = nodeId;
        this.partitionId = -1;
        this.partitionDepth = -1;
        this.numberOfEntries =-1;
        this.isLeader = false;
    }

    @Override
    public String toString() {
        return "SimpleDataModel{" +
                "nodeId=" + nodeId +
                ", partitionId=" + partitionId +
                ", partitionDepth=" + partitionDepth +
                ", numberOfEntries=" + numberOfEntries +
                ", isLeader=" + isLeader +
                '}';
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

    public boolean isLeader() {
        return isLeader;
    }

    public void setNodeId(int nodeId) {
        this.nodeId = nodeId;
    }

    public void setPartitionId(int partitionId) {
        this.partitionId = partitionId;
    }

    public void setPartitionDepth(int partitionDepth) {
        this.partitionDepth = partitionDepth;
    }

    public void setNumberOfEntries(long numberOfEntries) {
        this.numberOfEntries = numberOfEntries;
    }

    public void setLeader(boolean isLeader) {
        this.isLeader = isLeader;
    }
}
