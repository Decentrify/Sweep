package se.sics.ms.data;

import se.sics.ktoolbox.aggregator.util.PacketInfo;
import se.sics.p2ptoolbox.util.network.impl.DecoratedAddress;

/**
 * Packet representing the internal state of the 
 * node in the system.
 * 
 * Created by babbarshaer on 2015-09-09.
 */
public class InternalStatePacket implements PacketInfo {
    
    private int partitionId;
    private int partitionDepth;
    private DecoratedAddress leaderAddress;
    private long numEntries;
    
    public InternalStatePacket(int partitionId, int partitionDepth, DecoratedAddress leaderAddress, long numEntries){
        
        this.partitionId = partitionId;
        this.partitionDepth = partitionDepth;
        this.leaderAddress = leaderAddress;
        this.numEntries = numEntries;
    }

    @Override
    public String toString() {
        return "InternalStatePacket{" +
                "partitionId=" + partitionId +
                ", partitionDepth=" + partitionDepth +
                ", leaderAddress=" + leaderAddress +
                ", numEntries=" + numEntries +
                '}';
    }

    public int getPartitionId() {
        return partitionId;
    }

    public int getPartitionDepth() {
        return partitionDepth;
    }

    public DecoratedAddress getLeaderAddress() {
        return leaderAddress;
    }

    public long getNumEntries() {
        return numEntries;
    }
    
}
