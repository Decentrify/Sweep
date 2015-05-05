package se.sics.ms.util;

import org.javatuples.Pair;
import se.sics.ms.types.EpochUpdate;
import java.util.HashMap;
import java.util.Map;

/**
 * Entry tracker for the smallest missing entry in the application.
 *
 * @author babbarshaer
 */
public class MissingEntryTracker {

    private EpochUpdate currentEpochUpdate;
    private Map<Long, org.javatuples.Pair<Integer, Long>> entriesAdded;
    private long missingEntryId;

    public MissingEntryTracker(){
        entriesAdded = new HashMap<Long, Pair<Integer, Long>>();
    }
    
    public void trackEpochUpdate(EpochUpdate epochUpdate){
        this.currentEpochUpdate = epochUpdate;
        this.missingEntryId = 0;
    }

    public void updateEntriesAndClose(long numEntries){
        this.currentEpochUpdate.setEntriesAndCloseUpdate(numEntries);
    }

    /**
     * Is it safe to close the current epoch and switch to
     * tracking a new epoch update.
     *
     * @return True if all missing entries downloaded.
     */
    public boolean isCurrentEpochComplete(){
        
        if(currentEpochUpdate != null && currentEpochUpdate.getEpochUpdateStatus().equals(EpochUpdate.Status.COMPLETED) && missingEntryId >= (currentEpochUpdate.getNumEntries() -1)){
            return true;
        }
        return false;
    }
    
}
