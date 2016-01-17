package se.sics.ms.data;

import se.sics.ktoolbox.aggregator.util.AggregatorPacket;

/**
 * Packet representing the internal state of the 
 * node in the system.
 * 
 * Created by babbarshaer on 2015-09-09.
 */
public class InternalStatePacket implements AggregatorPacket {

    private Integer selfIdentifier;
    private int partitionId;
    private int partitionDepth;
    private Integer leaderAddress;
    private long numEntries;
    
    public InternalStatePacket(int selfIdentifier, int partitionId, int partitionDepth, Integer leaderAddress, long numEntries){

        this.selfIdentifier = selfIdentifier;
        this.partitionId = partitionId;
        this.partitionDepth = partitionDepth;
        this.leaderAddress = leaderAddress;
        this.numEntries = numEntries;
    }

    @Override
    public String toString() {
        return "InternalStatePacket{" +
                "selfIdentifier=" + selfIdentifier +
                ", partitionId=" + partitionId +
                ", partitionDepth=" + partitionDepth +
                ", leaderAddress=" + leaderAddress +
                ", numEntries=" + numEntries +
                '}';
    }

    public Integer getSelfIdentifier(){
        return selfIdentifier;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public int getPartitionDepth() {
        return partitionDepth;
    }

    public Integer getLeaderIdentifier() {
        return leaderAddress;
    }

    public long getNumEntries() {
        return numEntries;
    }
    
}
