package se.sics.ms.types;

/**
 * Marker class for the epoch information.
 * 
 *
 * Created by babbarshaer on 2015-05-20.
 */
public abstract class EpochContainer {

    protected final long epochId;
    protected final int leaderId;
    protected long numEntries;
    protected Status epochUpdateStatus;

    public EpochContainer (long epochId, int leaderId){

        this.epochId = epochId;
        this.leaderId = leaderId;
        this.numEntries = 0;
        this.epochUpdateStatus = Status.ONGOING;
    }

    public EpochContainer(long epochId, int leaderId, long numEntries){

        this.epochId = epochId;
        this.leaderId = leaderId;
        this.numEntries = numEntries;
        this.epochUpdateStatus = Status.COMPLETED;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EpochContainer that = (EpochContainer) o;

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

    public static enum Status {
        ONGOING,
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

    public Status getEpochUpdateStatus() {
        return epochUpdateStatus;
    }
}
