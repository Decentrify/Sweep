package se.sics.ms.util;

import se.sics.ms.types.LeaderUnit;

import java.util.*;

/**
 * Main Container for the leader unit histories contained in the node.
 * The application mainly communicates with the object in terms of 
 * storing the leader unit updates received by the application through the  
 * leader change or through the control pull mechanism.
 * 
 * Created by babbarshaer on 2015-05-25.
 */
public class TimeLine {
    
    private Map<Long, Epoch> epochMap;
    private LongComparator longComparator;
    private long maxEpochId;
    private static final Long INITIAL_EPOCH_ID = 0l;
    private LeaderUnit ongoingLeaderUnit;
    
    public TimeLine(){
        
        this.epochMap = new HashMap<Long, Epoch>();
        this.longComparator = new LongComparator();
        this.maxEpochId = -1;
    }


    public boolean isSafeToAdd(LeaderUnit leaderUnit) {

        if(maxEpochId != -1){
            
            long expectedEpochId = maxEpochId + 1;
            return ( leaderUnit.getEpochId() == maxEpochId 
                    || leaderUnit.getEpochId() == expectedEpochId);
        }
        
        return (leaderUnit.getEpochId() == 0);
    }
    
    
    
    public void addLeaderUnit (LeaderUnit leaderUnit) {
        
        Long epochId = leaderUnit.getEpochId();
        Epoch epoch = epochMap.get(epochId);
        
        if(epoch == null){
            epoch = new Epoch(epochId);
        }
        
        epoch.addLeaderUnit(leaderUnit);
        maxEpochId = leaderUnit.getEpochId();
    }
    
    
    public void rescanTimeLine(){
        throw new UnsupportedOperationException("Operation not supported.");
    }


    /**
     * Check for the update of the self tracking unit.
     * Update mainly involves the change in the leader unit status.
     *
     * @param leaderUnit base leader unit.
     * @return updated unit.
     */
    public LeaderUnit getSelfUnitUpdate(LeaderUnit leaderUnit){
        
        LeaderUnit result = null;
        
        long epochId = leaderUnit == null ? INITIAL_EPOCH_ID
                : leaderUnit.getEpochId();
        
        Epoch epoch = epochMap.get(epochId);
        if(epoch != null){

            List<LeaderUnit> units = epoch.getLeaderUnits();
            
            if(leaderUnit == null) {
                
                if(!units.isEmpty()){
                    result = units.get(0);
                }
            }
            
            else{
                
                int index = -1;
                for(int i =0; i < units.size(); i++){
                    
                    LeaderUnit lu = units.get(i);
                    if(lu.getLeaderId() == leaderUnit.getLeaderId()){
                        index = i;
                        break;
                    }
                }
                
                if(index != -1){
                    result = units.get(index);
                }
            }
            
                
        }
        return result;
    }
    

    /**
     * Iterate over the leader units in the timeline for different epochs.
     * Fetch them in order and add to the result list.
     *
     * @param baseUnit base unit
     * @param limit limit
     *
     * @return collection.
     */
    public List<LeaderUnit> getNextLeaderUnits(LeaderUnit baseUnit, int limit){

        List<LeaderUnit> units = new ArrayList<LeaderUnit>();
        
        int count = 0;
        LeaderUnit result = getNextUnitInOrder(baseUnit, false);
        
        while(result != null && count < limit){
            
            LeaderUnit copy = result.shallowCopy();
            result.setEntryPullStatus(LeaderUnit.EntryPullStatus.PENDING);
            units.add(result);
            
            result = getNextUnitInOrder(copy, false);
            count ++;
        }
        
        return units;
    }


    /**
     * Once the application has fetched all the entries in the leader unit container,
     * the application needs to inform the timeline that the unit fetch is complete.
     *
     * @param leaderUnit Current Leader Unit.
     * @return Completed Leader Unit.
     */
    public LeaderUnit markUnitComplete (LeaderUnit leaderUnit) {
        
        Epoch epoch;
        epoch = epochMap.get(leaderUnit.getEpochId());
        
        LeaderUnit result;
        
        if(epoch == null){
            throw new IllegalStateException("Unable to find Epoch for completed Leader Unit ...");
        }
        
        int index = -1;
        List<LeaderUnit> leaderUnits = epoch.getLeaderUnits();
        
        for(int i =0; i < leaderUnits.size(); i++){
            
            LeaderUnit lu = leaderUnits.get(i);
            if(lu.getEpochId() == leaderUnit.getEpochId() 
                    && lu.getLeaderId() == leaderUnit.getLeaderId() 
                    && lu.getLeaderUnitStatus() == LeaderUnit.LUStatus.COMPLETED){

                index = i;
                break;
            }
        }
        
        if(index == -1){
            throw new IllegalStateException("Unable to locate the completed leader unit inside the Epoch");
        }
        
        result = leaderUnits.get(index);
        result.setEntryPullStatus(LeaderUnit.EntryPullStatus.COMPLETED);
        leaderUnits.set(index, result);
        
        return result.shallowCopy(); // Return shallow copy so that application not able to change status.
    }


    /**
     * The application moves forward by fetching the next update to track.
     * The application
     *
     * @param leaderUnit Base LE Unit.
     * @return Next Unit to Track.
     */
    public LeaderUnit getNextUnitToTrack(LeaderUnit leaderUnit){

        LeaderUnit result;
        
        if(leaderUnit == null){
            return null;
        }
        
        if(( leaderUnit.getEntryPullStatus() != LeaderUnit.EntryPullStatus.COMPLETED )) {
            
            throw new IllegalStateException("Can't fetch next update as the leader unit is not properly closed.");
        }

        result = getNextInOrderPending(leaderUnit);
        
        if(result != null){
            
            LeaderUnit originalUnit = result;
            result = originalUnit.shallowCopy();
            
            result.setEntryPullStatus(LeaderUnit.EntryPullStatus.ONGOING);
            updateLeaderUnitLocally(originalUnit, result);
            ongoingLeaderUnit = result;
        }
        
        return result;
    }


    /**
     * Update the value of the leader unit locally to the updated value.
     *
     * @param originalLeaderUnit original
     * @param updatedValue updated value
     */
    private void updateLeaderUnitLocally (LeaderUnit originalLeaderUnit, LeaderUnit updatedValue){
        
        if(originalLeaderUnit == null || epochMap.get(originalLeaderUnit.getEpochId()) == null){
            throw new IllegalStateException("Unable to update the leader unit in timeline");
        }
        
        
        Epoch epoch = epochMap.get(originalLeaderUnit.getEpochId());
        List<LeaderUnit> units = epoch.getLeaderUnits();
        
        if(units.isEmpty() || !units.contains(originalLeaderUnit)){
            throw new IllegalStateException("Timeline history corrupted");
        }
        
        int index = units.indexOf(originalLeaderUnit);
        units.set(index, updatedValue);
    }
    
    
    
    /**
     * Simply a wrapper over the in order leader units.
     * In this case we not only need the next update but the next pending update.
     *
     * @param leaderUnit base unit.
     * @return next pending unit.
     */
    public LeaderUnit getNextInOrderPending(LeaderUnit leaderUnit){
        return this.getNextUnitInOrder(leaderUnit, true);
    }
    
    
    
    
    /**
     * Recursively go through the epochs and the associated leader units and 
     * fetch the next one to track.
     *  
     * Ideally there should always be only one ongoing leader unit in the history. 
     * It is the responsibility of the application to make sure that the value of the previous epoch is completed  
     * before starting the new one.
     *
     * @param leaderUnit base leader unit.
     *
     * @return next unit.
     */
    public LeaderUnit getNextUnitInOrder(LeaderUnit leaderUnit, boolean pending){

        Epoch epoch;
        epoch = (leaderUnit == null) ? epochMap.get(INITIAL_EPOCH_ID)
                : epochMap.get(leaderUnit.getEpochId());

        LeaderUnit result = null;
        
        if(epoch == null){
            return null;
        }

        List<LeaderUnit> leaderUnits = epoch.getLeaderUnits();
        if(!leaderUnits.isEmpty()) {
            
            if(leaderUnit == null){
                result = leaderUnits.get(0);        // Assuming that the Leader Units are always sorted.
            }
            
            else {
                
                int index = leaderUnits.indexOf(leaderUnit);
                if(index == -1){
                    throw new IllegalStateException(" Unable to locate leader unit entry ");
                }
                
                if( index == (leaderUnits.size() -1) ){     // The entry was the last one in the epoch.
                    Epoch nextEpoch = epochMap.get(leaderUnit.getEpochId() +1);
                    if(nextEpoch != null 
                            && !nextEpoch.getLeaderUnits().isEmpty()){
                        
                        result = getNextUnitInOrder( nextEpoch.getLeaderUnits().get(0), pending );
                    }
                }
                
                else {
                    
                    result = leaderUnits.get(index + 1);
                    
                    if( pending ){  
                        
                        // If Next In Line for the pending updates needs to be calculated.
                        if(result.getEntryPullStatus() != LeaderUnit.EntryPullStatus.PENDING) {
                            result = getNextUnitInOrder (result, true);
                        }
                    }
                    
                }
                
                
                
            }
        }
        
        return result;
    }


    /**
     * In case of sharding, we need to add the leader units to the skip list
     * as the application has already removed the information regarding the entries added as 
     * part of the shard update. 
     *
     * @param skipList skipList
     */
    public void addSkipList( List<LeaderUnit> skipList ){
        
        
        for(LeaderUnit unit : skipList){
            
            Epoch epoch = epochMap.get(unit.getEpochId());
            if(epoch != null){
                
                List<LeaderUnit> leaderUnits = epoch.getLeaderUnits();
                
                int index = -1;
                
                for(int i=0; i< leaderUnits.size() ; i++){
                    LeaderUnit lu = leaderUnits.get(i);
                    if(lu.getLeaderId() == unit.getLeaderId()){
                        index = i;
                        break;
                    }
                }
                
                if(index == -1){
                    throw new IllegalStateException("Unable to locate leader unit from skip list in local.");
                }
                
                LeaderUnit update = leaderUnits.get(index);
                update.setEntryPullStatus(LeaderUnit.EntryPullStatus.SKIP);
                
                leaderUnits.set(index, update);
                
            }
        }
        
        
        
    }


    /**
     * In case the application goes through sharding or any other major update,
     * the application can re-check that the update they were tracking is correct 
     * or they need to switch to a next update.
     *
     * @param unit base unit
     * @return
     */
    public boolean isTrackable(LeaderUnit unit){
        
        boolean result = false;
        
        Epoch epoch = epochMap.get(unit.getEpochId());
        
        if(epoch != null){
            
            int index = -1;
            List<LeaderUnit> units = epoch.getLeaderUnits();
            
            for(int i=0 ; i < units.size(); i ++){
                LeaderUnit lu = units.get(i);
                if(lu.getLeaderId() == unit.getLeaderId()){
                    index = i;
                    break;
                }
            }
            
            if(index != -1){
                
                LeaderUnit resultUnit = units.get(index);
                result = (resultUnit.getEntryPullStatus() != LeaderUnit.EntryPullStatus.COMPLETED
                        && resultUnit.getEntryPullStatus() != LeaderUnit.EntryPullStatus.SKIP);
                
            }
        }
        
        return result;
    }
    
    
    
    
    
    /**
     * Simple Comparator for the epochId's
     */
    private class LongComparator implements Comparator<Long>{
        
        @Override
        public int compare(Long o1, Long o2) {
            return o1.compareTo(o2);
        }
    }
    
    
}
