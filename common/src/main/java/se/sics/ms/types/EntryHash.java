package se.sics.ms.types;

import java.security.PublicKey;

/**
 * Container for the hash information for an application entry that the nodes
 * needs to pull in order to get up to date.
 *
 * Created by babbar on 2015-05-13.
 */
public class EntryHash {

    private final ApplicationEntry.ApplicationEntryId entryId;
    private final PublicKey leaderKey;
    private final String hash;

    public EntryHash(ApplicationEntry.ApplicationEntryId entryId, PublicKey leaderKey, String hash){
        
        this.entryId = entryId;
        this.leaderKey = leaderKey;
        this.hash = hash;
    }

    public EntryHash(ApplicationEntry entry){
        
        this.entryId = entry.getApplicationEntryId();
        this.leaderKey = entry.getEntry().getLeaderId();
        this.hash = entry.getEntry().getHash();
    }
    

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof EntryHash)) return false;

        EntryHash entryHash = (EntryHash) o;

        if (entryId != null ? !entryId.equals(entryHash.entryId) : entryHash.entryId != null) return false;
        if (hash != null ? !hash.equals(entryHash.hash) : entryHash.hash != null) return false;
        if (leaderKey != null ? !leaderKey.equals(entryHash.leaderKey) : entryHash.leaderKey != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = entryId != null ? entryId.hashCode() : 0;
        result = 31 * result + (leaderKey != null ? leaderKey.hashCode() : 0);
        result = 31 * result + (hash != null ? hash.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "EntryHash{" +
                "entryId=" + entryId +
                ", leaderKey=" + leaderKey +
                ", hash='" + hash + '\'' +
                '}';
    }

    public ApplicationEntry.ApplicationEntryId getEntryId() {
        return entryId;
    }

    public PublicKey getLeaderKey() {
        return leaderKey;
    }

    public String getHash() {
        return hash;
    }
}
