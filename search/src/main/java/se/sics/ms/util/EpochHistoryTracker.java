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
    private LinkedList<EpochUpdate> bufferedEpochHistory;
    
    public EpochHistoryTracker(){
        logger.trace("Tracker Initialized .. ");
        epochUpdateHistory = new LinkedList<EpochUpdate>();
    }


    /**
     * Based on the last missing entry,
     * decide the epochId the application needs to close right now.
     *
     * @return
     */
    private long epochIdToFetch(){

        EpochUpdate lastUpdate = getLastUpdate();
        if(lastUpdate.equals(EpochUpdate.NONE))
            return START_EPOCH_ID;

        return lastUpdate.getEpochUpdateStatus() == EpochUpdate.Status.COMPLETED ? lastUpdate.getEpochId()+1 : lastUpdate.getEpochId();
    }


    /**
     * General Interface to add an epoch to the history.
     * In case it is epoch update is already present in the history, update the entry with the new one.
     * FIX : Identify the methodology in case the epoch update is ahead and is a partition merge update.
     *
     * @param epochUpdate Epoch Update.
     */
    public void addEpochUpdate(EpochUpdate epochUpdate) {

        if(epochUpdate == null || epochUpdate.equals(EpochUpdate.NONE)){
            logger.debug("Request to add default epoch update received, returning ... ");
            return;
        }

        System.exit(-1);

        long epochIdToFetch = epochIdToFetch();
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

        else if(epochUpdate.getEpochId() == epochIdToFetch){
            epochUpdateHistory.addLast(epochUpdate); // Only append the entries in order.
        }

        // Special Case of the Network Partitioning Merge, in which we have to collapse the history.
        else if(epochUpdate.getEpochId() > epochIdToFetch){
            logger.warn(" HANDLE Case of the Network Partitioning Merge In the System.");

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
                : EpochUpdate.NONE;
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

        if(update.equals(EpochUpdate.NONE)){
            return update;
        }

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


    /**
     * Based on the current epoch update,
     * get the next updates from the epoch history collection.
     *
     * @param current current update
     * @param limit Max updates to provide.
     * @return Successive Updates.
     */
    public List<EpochUpdate> getNextUpdates(EpochUpdate current, int limit) {

        List<EpochUpdate> nextUpdates = new ArrayList<EpochUpdate>();

        if (current.equals(EpochUpdate.NONE)) {
            current = getInitialEpochUpdate();
        }

        if(current != null && !current.equals(EpochUpdate.NONE)){

            int index = epochUpdateHistory.indexOf(current);
            if(index != -1){

                ListIterator<EpochUpdate> listIterator = epochUpdateHistory.listIterator(index);
                int count = 0;
                while(listIterator.hasNext() && count < limit){
                    nextUpdates.add(listIterator.next());
                    count ++;
                }
            }

            else{
                logger.debug("Unable to locate epoch requested:{}", current);
            }
        }

        return nextUpdates;
    }

    /**
     * The method should always add the epoch updates to the tracker in order.
     *
     * @param intersection
     */
    public void addEpochUpdates(List<EpochUpdate> intersection) {

        Collections.sort(intersection);

        for (EpochUpdate nextUpdate : intersection) {
            addEpochUpdate(nextUpdate);
        }
    }
}
