package se.sics.ms.helper;

/**
 * Final state concerning the entries at each node.
 *
 * Created by babbar on 2015-09-18.
 */
public class EntryFinalState implements FinalStateInfo{

    private long entries;

    public EntryFinalState (long entries){
        this.entries = entries;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof EntryFinalState)) return false;

        EntryFinalState that = (EntryFinalState) o;

        if (entries != that.entries) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return (int) (entries ^ (entries >>> 32));
    }

    @Override
    public String toString() {
        return "EntryFinalState{" +
                "entries=" + entries +
                '}';
    }
}
