package se.sics.ms.util;

import se.sics.ms.types.ApplicationEntry;

/**
 * Collection of the entry identifier and the
 * score of the query match.
 *
 * Created by babbar on 2015-07-15.
 */
public class EntryScorePair implements Comparable<EntryScorePair>{

    private ApplicationEntry entry;
    private float score;


    public EntryScorePair(ApplicationEntry entry, float score){
        this.entry = entry;
        this.score = score;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof EntryScorePair)) return false;

        EntryScorePair that = (EntryScorePair) o;

        if (Float.compare(that.score, score) != 0) return false;
        if (entry != null ? !entry.equals(that.entry) : that.entry != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = entry != null ? entry.hashCode() : 0;
        result = 31 * result + (score != +0.0f ? Float.floatToIntBits(score) : 0);
        return result;
    }


    @Override
    public String toString() {
        return "IdScorePair{" +
                "entry=" + entry +
                ", score=" + score +
                '}';
    }

    public ApplicationEntry getEntry() {
        return entry;
    }

    public float getScore() {
        return score;
    }


    @Override
    public int compareTo(EntryScorePair o) {

        int result = Float.compare(this.score, o.score);
        return (result==0) ? this.entry.getApplicationEntryId().compareTo(o.entry.getApplicationEntryId()) : result;    // Act as a tie breaker.
    }

}
