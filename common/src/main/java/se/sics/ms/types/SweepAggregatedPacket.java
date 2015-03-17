package se.sics.ms.types;

import se.sics.p2ptoolbox.aggregator.api.model.AggregatedStatePacket;

/**
 * Condensed State Packet Information of Sweep.
 *
 * Created by babbar on 2015-03-17.
 */

public class SweepAggregatedPacket implements AggregatedStatePacket{

    private int nodeId;
    private int partitionDepth;
    private int partitionId;
    private long numberOfEntries;


    public SweepAggregatedPacket(int nodeId, int partitionId, int partitionDepth, long numberOfEntries) {
        this.nodeId = nodeId;
        this.partitionId = partitionId;
        this.partitionDepth = partitionDepth;
        this.numberOfEntries = numberOfEntries;
    }

    public SweepAggregatedPacket(int nodeId){
        this(nodeId, 0, 0, 0);
    }

    @Override
    public String toString() {
        return "SweepAggregatedPacket{" +
                "nodeId=" + nodeId +
                ", partitionDepth=" + partitionDepth +
                ", partitionId=" + partitionId +
                ", numberOfEntries=" + numberOfEntries +
                '}';
    }

    public int getNodeId() {
        return nodeId;
    }

    public int getPartitionDepth() {
        return partitionDepth;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public long getNumberOfEntries() {
        return numberOfEntries;
    }

    public void setNodeId(int nodeId) {
        this.nodeId = nodeId;
    }

    public void setPartitionDepth(int partitionDepth) {
        this.partitionDepth = partitionDepth;
    }

    public void setPartitionId(int partitionId) {
        this.partitionId = partitionId;
    }

    public void setNumberOfEntries(long numberOfEntries) {
        this.numberOfEntries = numberOfEntries;
    }
}

