package se.sics.ms.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.ms.types.LeaderUnit;

import java.util.*;

/**
 * Main Container for the leader unit histories contained in the node.
 * The application mainly communicates with the object in terms of
 * storing the leader unit updates received by the application through the
 * leader change or through the control pull mechanism.
 * <p/>
 * Created by babbarshaer on 2015-05-25.
 */
public class TimeLine {

    private Map<Long, Epoch> epochMap;
    private LongComparator longComparator;
    private long maxEpochId;
    private static final Long INITIAL_EPOCH_ID = 0l;
    private LeaderUnit ongoingLeaderUnit;
    private Logger logger = LoggerFactory.getLogger(TimeLine.class);
    
    public TimeLine() {

        this.epochMap = new HashMap<Long, Epoch>();
        this.longComparator = new LongComparator();
        this.maxEpochId = -1;
    }


    public Map<Long, Epoch> getEpochMap() {
        return epochMap;
    }

    /**
     * Wipe the internal state clean byt removing all the data
     * associated with different epochs.
     */
    public void cleanInternalState() {
        this.epochMap.clear();
    }

    public boolean isSafeToAdd(LeaderUnit leaderUnit) {

        if (maxEpochId != -1) {

            long expectedEpochId = maxEpochId + 1;
            return (leaderUnit.getEpochId() == maxEpochId
                    || leaderUnit.getEpochId() == expectedEpochId);
        }

        return (leaderUnit.getEpochId() == 0);
    }


    /**
     * Look into the local unit history and add the leader unit
     * As the method is generic in sense that it can be used to update
     * the already existing entries also, so check to find the unit
     * should be based on the epoch and
     *
     * @param leaderUnit
     */
    public void addLeaderUnit(LeaderUnit leaderUnit) {


        if (leaderUnit == null) {
            logger.debug("Tried to add null leader unit .. ");
            return;
        }

        Long epochId = leaderUnit.getEpochId();
        Epoch epoch = epochMap.get(epochId);

        if (epoch == null) {

            epoch = new Epoch(epochId);
            epochMap.put(epochId, epoch);
        }

        epoch.addLeaderUnit(leaderUnit.shallowCopy());
        maxEpochId = leaderUnit.getEpochId();
    }


    public void rescanTimeLine() {
        throw new UnsupportedOperationException("Operation not supported.");
    }


    /**
     * Check for the update of the self tracking unit.
     * Update mainly involves the change in the leader unit status.
     *
     * @param leaderUnit base leader unit.
     * @return updated unit.
     */
    public LeaderUnit getSelfUnitUpdate(LeaderUnit leaderUnit) {

        if (leaderUnit == null) {
            return null;
        }

        LeaderUnit result = leaderUnit;
        long epochId = leaderUnit.getEpochId();

        Epoch epoch = epochMap.get(epochId);
        if (epoch != null) {

            LeaderUnit lu = epoch.getLooseLeaderUnit(leaderUnit);
            if(lu  != null) result = lu;
        }

        return result;
    }


    /**
     * Special scenario in which the application has just booted up and therefore
     * looking for information to track.
     *
     * @return
     */
    public LeaderUnit getInitialTrackingUnit() {

        Epoch epoch = epochMap.get(INITIAL_EPOCH_ID);
        if (epoch == null) {
            return null;
        }

        return epoch.getFirstUnit();
    }


    /**
     * Iterate over the leader units in the timeline for different epochs.
     * Fetch them in order and add to the result list.
     *
     * @param baseUnit base unit
     * @param limit    limit
     * @return collection.
     */
    public List<LeaderUnit> getNextLeaderUnits(LeaderUnit baseUnit, int limit) {

        List<LeaderUnit> units = new ArrayList<LeaderUnit>();

        int count = 0;
        LeaderUnit result = getNextUnitInOrder(baseUnit, false);

        while (result != null && count < limit) {

            LeaderUnit copy = result.shallowCopy();
            result.setEntryPullStatus(LeaderUnit.EntryPullStatus.PENDING);
            units.add(result);

            result = getNextUnitInOrder(copy, false);
            count++;
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
    public LeaderUnit markUnitComplete(LeaderUnit leaderUnit) {

        Epoch epoch;
        epoch = epochMap.get(leaderUnit.getEpochId());

        LeaderUnit result;

        if (epoch == null) {
            throw new IllegalStateException("Unable to find Epoch for completed Leader Unit ...");
        }

        int index = -1;
        List<LeaderUnit> leaderUnits = epoch.getLeaderUnits();

        for (int i = 0; i < leaderUnits.size(); i++) {

            LeaderUnit lu = leaderUnits.get(i);
            if (lu.getEpochId() == leaderUnit.getEpochId()
                    && lu.getLeaderId() == leaderUnit.getLeaderId()
                    && lu.getLeaderUnitStatus() == LeaderUnit.LUStatus.COMPLETED) {

                index = i;
                break;
            }
        }

        if (index == -1) {
            throw new IllegalStateException("Unable to locate the completed leader unit inside the Epoch");
        }

        result = leaderUnits.get(index);
        result.setEntryPullStatus(LeaderUnit.EntryPullStatus.COMPLETED);

        return result.shallowCopy(); // Return shallow copy so that application not able to change status.
    }


    /**
     * The application moves forward by fetching the next update to track.
     * The unit should be next in line pending update. In case of a partition merge
     * there might be several completed updates in between because the class
     * calls a rescan method to rescan whole of timeline.
     *
     * @param leaderUnit Base LE Unit.
     * @return Next Unit to Track.
     */
    public LeaderUnit getNextUnitToTrack(LeaderUnit leaderUnit) {

        LeaderUnit result;

        if (leaderUnit == null) {
            return null;
        }

        if ((leaderUnit.getEntryPullStatus() == LeaderUnit.EntryPullStatus.COMPLETED
        || leaderUnit.getEntryPullStatus() == LeaderUnit.EntryPullStatus.SKIP)) {

            result = getNextInOrderPending(leaderUnit);
            if (result != null) {

                LeaderUnit originalUnit = result;
                result = originalUnit.shallowCopy();
            }
        }
        else{

            throw new IllegalStateException("Can't fetch next update as the leader unit is not properly closed.");
        }

        return result;
    }


    /**
     * Once the application decides to track an entry a call to this method is necessary, as it
     * will update the status of the entry being tracked in the main history tracker.
     * <p/>
     * The method checks on the current status of the entry to determine
     * if it can be tracked or not.
     *
     * @param leaderUnit
     * @return
     */
    public LeaderUnit currentTrackUnit(LeaderUnit leaderUnit) {

        Epoch epoch = epochMap.get(leaderUnit.getEpochId());

        if (epoch == null || !epoch.exactContainsCheck(leaderUnit)) {
            logger.warn("Unable to locate the entry in the store. ");
            return null;
        }
        if (leaderUnit.getEntryPullStatus() != LeaderUnit.EntryPullStatus.PENDING) {
            logger.warn("Only Pending entries can be tracked, returning ... ");
            return null;
        }

        LeaderUnit result = null;
        LeaderUnit lu = epoch.getLeaderUnit(leaderUnit);

        if (lu != null) {
            lu.setEntryPullStatus(LeaderUnit.EntryPullStatus.ONGOING);
            result = lu.shallowCopy();
            ongoingLeaderUnit = result;
        }

        return result;
    }


    /**
     * Update the value of the leader unit locally to the updated value.
     *
     * @param originalLeaderUnit original
     * @param updatedValue       updated value
     */
    private void updateLeaderUnitLocally(LeaderUnit originalLeaderUnit, LeaderUnit updatedValue) {

        if (originalLeaderUnit == null || epochMap.get(originalLeaderUnit.getEpochId()) == null) {
            throw new IllegalStateException("Unable to update the leader unit in timeline");
        }


        Epoch epoch = epochMap.get(originalLeaderUnit.getEpochId());
        List<LeaderUnit> units = epoch.getLeaderUnits();

        if (units.isEmpty() || !units.contains(originalLeaderUnit)) {
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
    private LeaderUnit getNextInOrderPending(LeaderUnit leaderUnit) {
        return this.getNextUnitInOrder(leaderUnit, true);
    }

    /**
     * Recursively go through the epochs and the associated leader units and
     * fetch the next one to track.
     * <p/>
     * Ideally there should always be only one ongoing leader unit in the history.
     * It is the responsibility of the application to make sure that the value of the previous epoch is completed
     * before starting the new one.
     *
     * @param leaderUnit base leader unit.
     * @return next unit.
     */
    private LeaderUnit getNextUnitInOrder(LeaderUnit leaderUnit, boolean pending) {

        Epoch epoch;
        epoch = (leaderUnit == null) ? epochMap.get(INITIAL_EPOCH_ID)
                : epochMap.get(leaderUnit.getEpochId());

        LeaderUnit result = null;

        if (epoch == null) {
            return null;
        }

        List<LeaderUnit> leaderUnits = epoch.getLeaderUnits();
        if (!leaderUnits.isEmpty()) {

            if (leaderUnit == null) {
                result = leaderUnits.get(0);        // Assuming that the Leader Units are always sorted.
            } else {

//                leaderUnit = epoch.getLooseLeaderUnit(leaderUnit);
                int index = leaderUnits.indexOf(leaderUnit);

                if (index == -1) {
                    logger.debug("Leader Units for epoch:{} are :{} and I am trying to look for :{}", new Object[]{leaderUnit.getEpochId(), leaderUnits, leaderUnit});
                    throw new IllegalStateException(" Unable to locate leader unit entry ");
                }

                if (index == (leaderUnits.size() - 1)) {     // The entry was the last one in the epoch.
                    Epoch nextEpoch = epochMap.get(leaderUnit.getEpochId() + 1);
                    if (nextEpoch != null
                            && !nextEpoch.getLeaderUnits().isEmpty()) {

                        result = nextEpoch.getLeaderUnits().get(0);
                    }
                } else {

                    result = leaderUnits.get(index + 1);
                }

                if (pending && (result != null)) {

                    // If Next In Line for the pending updates needs to be calculated.
                    if (result.getEntryPullStatus() != LeaderUnit.EntryPullStatus.PENDING) {
                        result = getNextInOrderPending(result);
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
    public void addSkipList(List<LeaderUnit> skipList) {


        for (LeaderUnit unit : skipList) {

            Epoch epoch = epochMap.get(unit.getEpochId());
            if (epoch != null) {

                List<LeaderUnit> leaderUnits = epoch.getLeaderUnits();

                int index = -1;

                for (int i = 0; i < leaderUnits.size(); i++) {
                    LeaderUnit lu = leaderUnits.get(i);
                    if (lu.getLeaderId() == unit.getLeaderId()) {
                        index = i;
                        break;
                    }
                }

                if (index == -1) {
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
    public boolean isTrackable(LeaderUnit unit) {

        boolean result = false;

        Epoch epoch = epochMap.get(unit.getEpochId());

        if (epoch != null) {

            LeaderUnit resultUnit = epoch.getLooseLeaderUnit(unit);
            if (resultUnit != null) {

                result = (resultUnit.getEntryPullStatus() != LeaderUnit.EntryPullStatus.COMPLETED
                        && resultUnit.getEntryPullStatus() != LeaderUnit.EntryPullStatus.SKIP);
            }

        }

        return result;
    }


    /**
     * Based on the loose interpretation of the LeaderUnit, check for a similar
     * one in the local store and return the matching value.
     *
     * @param leaderUnit base
     * @return matching unit.
     */
    public LeaderUnit getLooseUnit(LeaderUnit leaderUnit){

        if(leaderUnit == null){
            return null;
        }

        return this.getLooseUnit(leaderUnit.getEpochId(), leaderUnit.getLeaderId());
    }


    /**
     * Check for a similar leader unit in the local store and in case 
     * a match is found , return the value. 
     *
     * @param epochId unit epoch
     * @param leaderId unit leader
     * @return unit
     */
    public LeaderUnit getLooseUnit(long epochId, int leaderId){
        
        if(!this.epochMap.containsKey(epochId)){
            return null;
        }
        
        Epoch epoch = epochMap.get(epochId);
        return epoch.getLooseLeaderUnit(epochId, leaderId);
        
    }
    
    
    
    
    
    
    
    /**
     * Mainly used by the control pull mechanism in order
     * to determine the last leader unit in the system.
     * 
     * We can reduce the complexity of the method by keeping track at the addition time
     * of leader unit to epoch. For now use this method.
     *
     * @return Last Unit
     */
    public LeaderUnit getLastUnit() {

        LeaderUnit lastUnit = null;
        Set<Long> keySet = epochMap.keySet();

        if (!keySet.isEmpty()) {

            long highestEpoch;

            List<Long> keyList = new ArrayList<Long>(keySet);
            Collections.sort(keyList, longComparator);

            highestEpoch = keyList.get(keyList.size() - 1); // Last Entry in collection.
            lastUnit = epochMap.get(highestEpoch).getLastUpdate();
        }

        return lastUnit;
    }


    /**
     * Simple Comparator for the epochId's
     */
    private class LongComparator implements Comparator<Long> {

        @Override
        public int compare(Long o1, Long o2) {
            return o1.compareTo(o2);
        }
    }


}
