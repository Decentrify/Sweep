package se.sics.ms.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.ms.types.EpochUpdate;

import java.util.*;

/**
 * Stores and keep tracks of the epoch history.
 *  
 * No specific ordering is imposed here. It is the responsibility of the application to 
 * fetch the epoch updates in order by looking at the last added the epoch history.
 *
 * @author babbarshaer
 */
public class EpochHistoryTracker {
    
    private LinkedList<EpochUpdate> epochUpdateHistory;
    private static Logger logger = LoggerFactory.getLogger(EpochHistoryTracker.class);
    private static final int START_EPOCH_ID = 0;
    
    public EpochHistoryTracker(){
        logger.trace("Tracker Initialized .. ");
        epochUpdateHistory = new LinkedList<EpochUpdate>();
    }

    /**
     * General Interface to add an epoch to the history.
     * In case it is epoch update is already present in the history, update the entry with the new one.
     *
     * @param epochUpdate Epoch Update.
     */
    public void addEpochUpdate(EpochUpdate epochUpdate) {
        
        int index = -1;
        for(int i =0; i < epochUpdateHistory.size() ; i ++){
            if(epochUpdateHistory.get(i).getEpochId() == epochUpdate.getEpochId() &&  
                    epochUpdateHistory.get(i).getLeaderId() == epochUpdate.getLeaderId()) {
                index = i;
                break;
            }
        }

        if (index != -1) {
            epochUpdateHistory.set(index, epochUpdate);
        }
        else{
            epochUpdateHistory.addLast(epochUpdate);
        }
        Collections.sort(epochUpdateHistory);   // SORT the collection before returning. ( SORTING based on Natural Ordering ).
    }

    /**
     * Get the last update that has been added to the history tracker.
     * The application needs this information to know where to pull from.
     * 
     * @return Epoch Update.
     */
    public EpochUpdate getLastUpdate(){
        
        return !this.epochUpdateHistory.isEmpty()
                ? this.epochUpdateHistory.getLast() 
                : null;
    }

    /**
     * Based on epoch update provided calculate the next epoch update that needs to be tracked by
     * the index pull mechanism.
     *  
     * @param update
     * @return
     */
    public EpochUpdate getNextUpdateToTrack(EpochUpdate update){
        
        EpochUpdate nextUpdate = null;
        Iterator<EpochUpdate> iterator = epochUpdateHistory.iterator();
        
        while(iterator.hasNext()){
            if(iterator.next().equals(update)){
                
                if(iterator.hasNext()){
                    nextUpdate = iterator.next();
                    break;
                }
            }
        }
        return nextUpdate;
    }

    /**
     * Check for any updates to the entry matching the value provided by the
     * application.
     *
     * @param update Update to match against.
     * @return Updated Value.
     */
    public EpochUpdate getSelfUpdate(EpochUpdate update){
        
        for(EpochUpdate epochUpdate : epochUpdateHistory){
            if(epochUpdate.getEpochId() == update.getEpochId() 
                    && epochUpdate.getLeaderId() == update.getLeaderId()){
                
                return epochUpdate;
            }
        }
        
        return null;
    }

    /**
     * Search for the update with the starting epochId.
     * Return the first known reference.
     * 
     * @return Initial Epoch Update.
     */
    public EpochUpdate getInitialEpochUpdate() {
        
        for(EpochUpdate update : epochUpdateHistory){
            if(update.getEpochId() == START_EPOCH_ID){
                return update;
            }
        }
        
        return null;
    }

}
