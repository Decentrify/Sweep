package se.sics.ms.types;

/**
 * Epoch Update contains metadata associated with the evolution of the system
 * with new leaders getting elected and adding entries to it.
 *
 * @author babbar
 */
public class EpochUpdate implements Comparable<EpochUpdate>{

    private final long epochId;
    private final int leaderId;
    private long numEntries;
    private Status epochUpdateStatus;

    public EpochUpdate (long epochId, int leaderId){

        this.epochId = epochId;
        this.leaderId = leaderId;
        this.numEntries = 0;
        this.epochUpdateStatus = Status.ONGOING;
    }

    public EpochUpdate(long epochId, int leaderId, long numEntries){

        this.epochId = epochId;
        this.leaderId = leaderId;
        this.numEntries = numEntries;
        this.epochUpdateStatus = Status.COMPLETED;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EpochUpdate that = (EpochUpdate) o;

        if (epochId != that.epochId) return false;
        if (leaderId != that.leaderId) return false;
        if (numEntries != that.numEntries) return false;
        if (epochUpdateStatus != that.epochUpdateStatus) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (epochId ^ (epochId >>> 32));
        result = 31 * result + leaderId;
        result = 31 * result + (int) (numEntries ^ (numEntries >>> 32));
        result = 31 * result + (epochUpdateStatus != null ? epochUpdateStatus.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "EpochUpdate{" +
                "epochId=" + epochId +
                ", leaderId=" + leaderId +
                ", numEntries=" + numEntries +
                ", epochUpdateStatus=" + epochUpdateStatus +
                '}';
    }

    public void setEntriesAndCloseUpdate(long numEntries){

        this.numEntries = numEntries;
        this.epochUpdateStatus = Status.COMPLETED;
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

    public Status getEpochUpdateStatus() {
        return epochUpdateStatus;
    }

    
    @Override
    public int compareTo(EpochUpdate o) {
        
        if(o == null){
            throw new NullPointerException("Epoch Update to compare to is null");
        }
        
        int epochCompareResult = Long.valueOf(this.epochId).compareTo(o.epochId);
        if(epochCompareResult != 0){
            return epochCompareResult;
        }
        
        int leaderIdCompareResult = Integer.valueOf(this.leaderId).compareTo(o.leaderId);
        if(leaderIdCompareResult != 0){
            return leaderIdCompareResult;
        }
        
        return Long.valueOf(this.numEntries).compareTo(o.numEntries);
    }
    
    
    public static enum Status {
        ONGOING,
        COMPLETED
    }


}
