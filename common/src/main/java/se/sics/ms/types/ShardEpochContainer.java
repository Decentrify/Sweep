package se.sics.ms.types;

import java.security.PublicKey;

/**
 * Epoch Container for the Sharding Information.
 * Used for containing information that will be exchanged during
 * the sharding phase.
 *  
 * Created by babbarshaer on 2015-05-20.
 */
public class ShardEpochContainer extends EpochContainer{
    
    private ApplicationEntry.ApplicationEntryId medianId;
    private PublicKey leaderKey;
    private String hash;

    public ShardEpochContainer(long epochId, int leaderId, long numEntries, ApplicationEntry.ApplicationEntryId medianId, PublicKey leaderKey) {
        super(epochId, leaderId, numEntries);
        this.medianId = medianId;
        this.leaderKey = leaderKey;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        ShardEpochContainer that = (ShardEpochContainer) o;

        if (hash != null ? !hash.equals(that.hash) : that.hash != null) return false;
        if (leaderKey != null ? !leaderKey.equals(that.leaderKey) : that.leaderKey != null) return false;
        if (medianId != null ? !medianId.equals(that.medianId) : that.medianId != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (medianId != null ? medianId.hashCode() : 0);
        result = 31 * result + (leaderKey != null ? leaderKey.hashCode() : 0);
        result = 31 * result + (hash != null ? hash.hashCode() : 0);
        return result;
    }

    public void setHash(String hash) {
        this.hash = hash;
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
