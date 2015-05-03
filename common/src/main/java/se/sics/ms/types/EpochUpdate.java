package se.sics.ms.types;

/**
 * Epoch Update contains metadata associated with the evolution of the system
 * with new leaders getting elected and adding entries to it.
 *
 * @author babbar
 */
public class EpochUpdate {

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

    public static enum Status {
        ONGOING,
        COMPLETED
    }


}
