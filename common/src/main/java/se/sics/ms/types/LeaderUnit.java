package se.sics.ms.types;

/**
 * Marker class for the epoch information.
 * 
 *
 * Created by babbarshaer on 2015-05-20.
 */
public abstract class LeaderUnit {

    protected final long epochId;
    protected final int leaderId;
    protected long numEntries;
    protected LUStatus leaderUnitStatus;
    protected transient EntryPullStatus entryPullStatus;

    public LeaderUnit(long epochId, int leaderId){

        this.epochId = epochId;
        this.leaderId = leaderId;
        this.numEntries = 0;
        this.leaderUnitStatus = LUStatus.ONGOING;
        this.entryPullStatus = EntryPullStatus.PENDING;
    }

    public LeaderUnit(long epochId, int leaderId, long numEntries){

        this.epochId = epochId;
        this.leaderId = leaderId;
        this.numEntries = numEntries;
        this.leaderUnitStatus = LUStatus.COMPLETED;
        this.entryPullStatus = EntryPullStatus.PENDING;
    }
    
    public LeaderUnit( long epochId, int leaderId, long numEntries, LUStatus leaderUnitStatus, EntryPullStatus entryPullStatus ){
        this.epochId = epochId;
        this.leaderId = leaderId;
        this.numEntries = numEntries;
        this.leaderUnitStatus = leaderUnitStatus;
        this.entryPullStatus = entryPullStatus;
        
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LeaderUnit that = (LeaderUnit) o;

        if (epochId != that.epochId) return false;
        if (leaderId != that.leaderId) return false;
        if (numEntries != that.numEntries) return false;
        if (leaderUnitStatus != that.leaderUnitStatus) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (epochId ^ (epochId >>> 32));
        result = 31 * result + leaderId;
        result = 31 * result + (int) (numEntries ^ (numEntries >>> 32));
        result = 31 * result + (leaderUnitStatus != null ? leaderUnitStatus.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "LeaderUnit{" +
                "epochId=" + epochId +
                ", leaderId=" + leaderId +
                ", numEntries=" + numEntries +
                ", leaderUnitStatus=" + leaderUnitStatus +
                ", entryPullStatus=" + entryPullStatus +
                '}';
    }

    public EntryPullStatus getEntryPullStatus() {
        return entryPullStatus;
    }

    public static enum LUStatus {
        
        ONGOING,
        COMPLETED
    }
    
    
    public static enum EntryPullStatus {
        
        PENDING,
        ONGOING,
        SKIP,
        COMPLETED
    }

    public long getEpochId() {
        return epochId;
    }

    public int getLeaderId() {
        return leaderId;
    }

    public long getNumEntries() {
        return numEntries;
    }

    public LUStatus getLeaderUnitStatus() {
        return leaderUnitStatus;
    }

    public void setEntryPullStatus(EntryPullStatus entryPullStatus) {
        this.entryPullStatus = entryPullStatus;
    }

    public void setLeaderUnitStatus(LUStatus leaderUnitStatus) {
        this.leaderUnitStatus = leaderUnitStatus;
    }

    public abstract LeaderUnit shallowCopy();
}
