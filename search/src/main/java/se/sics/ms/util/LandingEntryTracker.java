package se.sics.ms.util;

import java.util.UUID;

/**
 * Special Tracker for the landing entry.
 * Landing Entry addition helps in the evolution of the system and therefore the correct addition of this entry 
 * is of prime importance.
 *
 * Created by babbarshaer on 2015-05-05.
 */
public class LandingEntryTracker {
    
    private boolean isLandingEntryAdded;
    private long epochId;
    private UUID landingEntryRoundId;
    private long landingEntryId;
    
    public LandingEntryTracker(){
    }
    
    public void startTracking(long epochId, UUID landingEntryRoundId, long landingEntryId) {
        
        this.epochId = epochId;
        this.landingEntryRoundId = landingEntryRoundId;
        this.landingEntryId = landingEntryId;
    }

    public void resetTracker(){
        landingEntryRoundId = null;
        this.epochId = 0;
    }
    
    public boolean isLandingEntryAdded() {
        return isLandingEntryAdded;
    }

    public long getEpochId() {
        return epochId;
    }

    public UUID getLandingEntryRoundId() {
        return landingEntryRoundId;
    }
    
    public long getLandingEntryId(){
        return this.landingEntryId;
    }
}
