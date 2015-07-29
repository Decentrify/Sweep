package se.sics.ms.util;

import se.sics.ms.types.ApplicationEntry;

/**
 * Collection of the entry identifier and the
 * score of the query match.
 *
 * Created by babbar on 2015-07-15.
 */
public class IdScorePair implements Comparable<IdScorePair>{

    private ApplicationEntry.ApplicationEntryId entryId;
    private float score;


    public IdScorePair(ApplicationEntry.ApplicationEntryId entryId, float score){
        this.entryId = entryId;
        this.score = score;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof IdScorePair)) return false;

        IdScorePair that = (IdScorePair) o;

        if (Float.compare(that.score, score) != 0) return false;
        if (entryId != null ? !entryId.equals(that.entryId) : that.entryId != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = entryId != null ? entryId.hashCode() : 0;
        result = 31 * result + (score != +0.0f ? Float.floatToIntBits(score) : 0);
        return result;
    }


    @Override
    public String toString() {
        return "IdScorePair{" +
                "entryId=" + entryId +
                ", score=" + score +
                '}';
    }

    public ApplicationEntry.ApplicationEntryId getEntryId() {
        return entryId;
    }

    public float getScore() {
        return score;
    }


    @Override
    public int compareTo(IdScorePair o) {

        int result = Float.compare(this.score, o.score);
        return (result==0) ? this.entryId.compareTo(o.entryId) : result;    // Act as a tie breaker.
    }

}
