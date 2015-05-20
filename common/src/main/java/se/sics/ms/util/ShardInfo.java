package se.sics.ms.util;

import se.sics.ms.types.ApplicationEntry;
import java.security.PublicKey;
import java.util.UUID;

/**
 * Container for the information contained that the leader sends to the 
 * leader group nodes in the event of sharding.
 * 
 * Created by babbarshaer on 2015-05-20.
 */
public class ShardInfo {

    private UUID shardRoundId;
    private ApplicationEntry.ApplicationEntryId medianId;
    private PublicKey leaderKey;
    private String hash;
    
    
    public ShardInfo(UUID shardRoundId, ApplicationEntry.ApplicationEntryId medianId, PublicKey leaderKey){
        this.shardRoundId = shardRoundId;
        this.medianId = medianId;
        this.leaderKey = leaderKey;
    }

    @Override
    public String toString() {
        return "ShardInfo{" +
                "shardRoundId=" + shardRoundId +
                ", medianId=" + medianId +
                ", leaderKey=" + leaderKey +
                ", hash='" + hash + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ShardInfo shardInfo = (ShardInfo) o;

        if (hash != null ? !hash.equals(shardInfo.hash) : shardInfo.hash != null) return false;
        if (leaderKey != null ? !leaderKey.equals(shardInfo.leaderKey) : shardInfo.leaderKey != null) return false;
        if (medianId != null ? !medianId.equals(shardInfo.medianId) : shardInfo.medianId != null) return false;
        if (shardRoundId != null ? !shardRoundId.equals(shardInfo.shardRoundId) : shardInfo.shardRoundId != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = shardRoundId != null ? shardRoundId.hashCode() : 0;
        result = 31 * result + (medianId != null ? medianId.hashCode() : 0);
        result = 31 * result + (leaderKey != null ? leaderKey.hashCode() : 0);
        result = 31 * result + (hash != null ? hash.hashCode() : 0);
        return result;
    }

    public void setHash(String hash) {
        this.hash = hash;
    }

    public UUID getShardRoundId() {
        return shardRoundId;
    }

    public ApplicationEntry.ApplicationEntryId getMedianId() {
        return medianId;
    }

    public PublicKey getLeaderKey() {
        return leaderKey;
    }

    public String getHash() {
        return hash;
    }
}
