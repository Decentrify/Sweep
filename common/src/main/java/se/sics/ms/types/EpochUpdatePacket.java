package se.sics.ms.types;

/**
 * Packet containing information regarding the epoch updates 
 * provided by the node trying to push an epoch update. 
 *  
 * Created by babbarshaer on 2015-05-20.
 */
public class EpochUpdatePacket {
    
    private EpochContainer previousEpochUpdate;
    private EpochContainer currentEpochUpdate;

    public EpochUpdatePacket(EpochContainer previousEpochUpdate, EpochContainer currentEpochUpdate) {
        this.previousEpochUpdate = previousEpochUpdate;
        this.currentEpochUpdate = currentEpochUpdate;
    }

    @Override
    public String toString() {
        return "EpochUpdatePacket{" +
                "previousEpochUpdate=" + previousEpochUpdate +
                ", currentEpochUpdate=" + currentEpochUpdate +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EpochUpdatePacket that = (EpochUpdatePacket) o;

        if (currentEpochUpdate != null ? !currentEpochUpdate.equals(that.currentEpochUpdate) : that.currentEpochUpdate != null)
            return false;
        if (previousEpochUpdate != null ? !previousEpochUpdate.equals(that.previousEpochUpdate) : that.previousEpochUpdate != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = previousEpochUpdate != null ? previousEpochUpdate.hashCode() : 0;
        result = 31 * result + (currentEpochUpdate != null ? currentEpochUpdate.hashCode() : 0);
        return result;
    }

    public EpochContainer getPreviousEpochUpdate() {
        return previousEpochUpdate;
    }

    public EpochContainer getCurrentEpochUpdate() {
        return currentEpochUpdate;
    }
}
