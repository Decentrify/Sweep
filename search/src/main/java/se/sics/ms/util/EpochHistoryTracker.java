package se.sics.ms.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.ms.types.LeaderUnit;
import se.sics.p2ptoolbox.util.network.impl.BasicAddress;

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

    private LinkedList<LeaderUnit> epochUpdateHistory;
    private static Logger logger = LoggerFactory.getLogger(EpochHistoryTracker.class);
    private static final int START_EPOCH_ID = 0;
    private LinkedList<LeaderUnit> bufferedEpochHistory;
    private BasicAddress selfAddress;
    private String prefix;
    private GenericECComparator comparator;
    private ArrayList<LeaderUnit> skipEpochHistory;
    
    public EpochHistoryTracker(BasicAddress address){

        logger.trace("Tracker Initialized .. ");
        epochUpdateHistory = new LinkedList<LeaderUnit>();
        bufferedEpochHistory = new LinkedList<LeaderUnit>();
        skipEpochHistory = new ArrayList<LeaderUnit>();
        comparator = new GenericECComparator();
        
        this.selfAddress = address;
        this.prefix = String.valueOf(address.getId());
    }



    public void printEpochHistory(){
        logger.warn("EpochHistory: {}", epochUpdateHistory);
    }

    /**
     * Based on the last missing entry,
     * decide the epochId the application needs to close right now.
     *
     * @return next epoch id to ask.
     */
    private long epochIdToFetch(){

        LeaderUnit lastUpdate = getLastUpdate();
        
        return lastUpdate == null 
                ? START_EPOCH_ID 
                :((lastUpdate.getLeaderUnitStatus() == LeaderUnit.LUStatus.COMPLETED)
                        ? lastUpdate.getEpochId()+1 
                        : lastUpdate.getEpochId());
    }


    /**
     * General Interface to add an epoch to the history.
     * In case it is epoch update is already present in the history, update the entry with the new one.
     * FIX : Identify the methodology in case the epoch update is ahead and is a partition merge update.
     * FIX : Identify the methodology in case the epoch update is a part of shard update.
     *
     * @param epochUpdate Epoch Update.
     */
    public void addEpochUpdate(LeaderUnit epochUpdate) {

        if( epochUpdate == null ){
            logger.debug("Request to add default epoch update received, returning ... ");
            return;
        }

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

            logger.debug("{}: Going to add new epoch update :{} ", prefix, epochUpdate);
            epochUpdateHistory.addLast(epochUpdate); // Only append the entries in order.
        }

        // Special Case of the Network Partitioning Merge, in which we have to collapse the history.
        else if(epochUpdate.getEpochId() > epochIdToFetch){

            // Special Case Handling for the Network Merge is required.
            logger.error(" HANDLE Case of the Network Partitioning Merge In the System.");
            bufferedEpochHistory.add(epochUpdate);              // TO DO: Condition needs to be properly handled.
            throw new UnsupportedOperationException(" Operation Not Supported Yet ");
        }

        else{
            logger.warn("{}: Whats the case that occurred : ?", prefix);
            throw new IllegalStateException("Unknown State ..1");
        }

    }

    /**
     * Get the last update that has been added to the history tracker.
     * The application needs this information to know where to pull from.
     * 
     * @return Epoch Update.
     */
    public LeaderUnit getLastUpdate(){
        
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
    public LeaderUnit getNextUpdateToTrack(LeaderUnit update){
        
        LeaderUnit nextUpdate = null;
        Iterator<LeaderUnit> iterator = epochUpdateHistory.iterator();
        
        while(iterator.hasNext()){

            if(iterator.next().equals(update))
            {
                if(iterator.hasNext())
                {
                    nextUpdate = iterator.next();
                    break;                              // Check Here About the Buffered Epoch Updates in The System and Remove them from the
                }
            }
        }

        // Check in skipList.
        if(nextUpdate != null && skipEpochHistory.contains(nextUpdate)){
            
            skipEpochHistory.remove(nextUpdate);
            nextUpdate = getNextUpdateToTrack(nextUpdate);
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
    public LeaderUnit getSelfUpdate(LeaderUnit update){

        if( update == null ){
            return null;
        }

        for(LeaderUnit epochUpdate : epochUpdateHistory){
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
    public LeaderUnit getInitialEpochUpdate() {
        
        for(LeaderUnit update : epochUpdateHistory){
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
    public List<LeaderUnit> getNextUpdates(LeaderUnit current, int limit) {

        List<LeaderUnit> nextUpdates = new ArrayList<LeaderUnit>();

        if (current == null) {
            
            current = getInitialEpochUpdate();
            if(current != null) {
                nextUpdates.add(current);
            }
        }
        else{
            current = getSelfUpdate(current);
        }

        if( current != null && !current.getLeaderUnitStatus().equals(LeaderUnit.LUStatus.ONGOING)){

            // Needs to be updated in case of partition merge as the update might not be present due to sewing up of history.
            // Also the direct equals method won't work in case of multiple types of epoch updates in the system.

            int index = epochUpdateHistory.indexOf(current);        
            if(index != -1){

                ListIterator<LeaderUnit> listIterator = epochUpdateHistory.listIterator(index);
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
     * This method delegates the responsibility to the common method inside the
     * class to add the updates but also to check if they are in order or not.
     *
     * @param intersection Collection.
     */
    public void addEpochUpdates(List<LeaderUnit> intersection) {

        Collections.sort(intersection, comparator );

        for (LeaderUnit nextUpdate : intersection) {
            addEpochUpdate(nextUpdate);
        }
    }


    /**
     * Collection of updates that the entry pull mechanism needs to skip.
     * So these are buffered in epoch history to be removed when pull mechanism reaches that point.
     *
     * @param skipUpdateCollection collection.
     */
    public void addSkipList(Collection<LeaderUnit> skipUpdateCollection){
        skipEpochHistory.addAll(skipUpdateCollection);
    }
    
    
    
}
