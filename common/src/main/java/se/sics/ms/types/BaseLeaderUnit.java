package se.sics.ms.types;

/**
 * Simple Implementation of the shard container.
 * Formally Used when a node becomes the leader and takes further the 
 * epoch update cycle.
 *
 * Created by babbarshaer on 2015-05-20.
 */
public class BaseLeaderUnit extends LeaderUnit {

    public BaseLeaderUnit(long epochId, int leaderId) {
        super(epochId, leaderId);
    }

    public BaseLeaderUnit(long epochId, int leaderId, long numEntries) {
        super(epochId, leaderId, numEntries);
    }

    public BaseLeaderUnit(long epochId, int leaderId, long numEntries, LUStatus leaderUnitStatus, EntryPullStatus entryPullStatus) {
        
        super(epochId, leaderId, numEntries, leaderUnitStatus, entryPullStatus);
    }

    @Override
    public String toString() {
        return super.toString();
    }

    @Override
    public LeaderUnit shallowCopy() {
        return new BaseLeaderUnit(this.epochId, this.leaderId, this.numEntries, this.leaderUnitStatus, this.entryPullStatus);
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }
}
