package search.system.peer.util;

import se.sics.ms.common.LuceneAdaptorException;
import se.sics.ms.types.ApplicationEntry;
import se.sics.ms.types.BaseEpochContainer;
import se.sics.ms.types.EpochContainer;
import se.sics.ms.types.ShardEpochContainer;

import java.io.IOException;
import java.util.*;

/**
 * Testing the applicability of the shard tracker.
 * <p/>
 * Created by babbarshaer on 2015-05-25.
 */
public class ShardScenarioTest {

    private static LowestMissingTracker lowestMissingEntryTracker;
    private static EpochHistoryTracker epochHistoryTracker;
    private static int leaderId = 100;
    private static long closedContainerSize = 100;
    

    public static void main(String[] args) throws IOException, LuceneAdaptorException {

        int numEpochContainers = 10;
        
        lowestMissingEntryTracker = new LowestMissingTracker();
        epochHistoryTracker = new EpochHistoryTracker();

        epochHistoryTracker.setEpochHistory(createClosedEpochHistoryList(leaderId, 0, 5));
        epochHistoryTracker.setEpochHistory(createClosedEpochHistoryList(leaderId + 100, 5, 5));

        
        
        lowestMissingEntryTracker.setCurrentTrackingUpdate(new BaseEpochContainer(3, leaderId), 10);


        EpochContainer shardEpochContainer = new BaseEpochContainer(numEpochContainers, leaderId, 1);
        ApplicationEntry.ApplicationEntryId medianId = new ApplicationEntry.ApplicationEntryId(6, leaderId + 100, 12);
        
        List<EpochContainer>skipList = generateSkipList(shardEpochContainer, medianId, false);
        System.out.println(" Skip List : " + skipList);
    }


    /**
     * Main shard method to test in the system.
     *
     * @param shardContainer
     * @param medianId
     * @param partitionSubId
     * @return
     * @throws IOException
     * @throws LuceneAdaptorException
     */
    private static List<EpochContainer> generateSkipList(EpochContainer shardContainer, ApplicationEntry.ApplicationEntryId medianId, boolean partitionSubId) throws IOException, LuceneAdaptorException {

        EpochContainer lastAddedUpdate = epochHistoryTracker.getLastUpdate();

        if (lastAddedUpdate == null || (lastAddedUpdate.getEpochId() >= shardContainer.getEpochId()
                && lastAddedUpdate.getLeaderId() >= shardContainer.getLeaderId())) {

            throw new IllegalStateException("Sharding State Corrupted ..  ");
        }

        EpochContainer container = lowestMissingEntryTracker.getCurrentTrackingUpdate();
        long currentId = lowestMissingEntryTracker.getEntryBeingTracked().getEntryId();

        List<EpochContainer> pendingUpdates = epochHistoryTracker.getNextUpdates(
                container,
                Integer.MAX_VALUE);

        Iterator<EpochContainer> iterator = pendingUpdates.iterator();

        // Based on which section of the entries that the nodes will clear
        // Update the pending list.

        if (partitionSubId) {
            // If right to the median id is removed, skip list should contain
            // entries to right of the median.
            while (iterator.hasNext()) {

                EpochContainer nextContainer = iterator.next();
//
//                if (!(nextContainer.getEpochId() >= medianId.getEpochId() &&
//                        nextContainer.getLeaderId() > medianId.getLeaderId())) {
//                    iterator.remove();
//                }
                
                if(nextContainer.getEpochId() <= medianId.getEpochId()){
                    
                    if(nextContainer.getEpochId() == medianId.getEpochId() 
                            &&nextContainer.getLeaderId() == medianId.getLeaderId()){
                        
                        break;
                    }

                    iterator.remove();
                }
                
                
                
                
            }
        } else {

            // If left to the median is removed, skip list should contain
            // entries to the left of the median.
            while (iterator.hasNext()) {

                EpochContainer nextContainer = iterator.next();

//                if (!(medianId.getEpochId() > nextContainer.getEpochId() &&
//                        medianId.getLeaderId() >= nextContainer.getLeaderId())) {
//                    iterator.remove();
//                }
                
                if(nextContainer.getEpochId() >= medianId.getEpochId()){
                    iterator.remove();
                }
                
            }
        }

        // Now based on the entries found, compare with the actual
        // state of the entry pull mechanism and remove the entries already fetched .
        Iterator<EpochContainer> remainingItr = pendingUpdates.iterator();
        while (remainingItr.hasNext()) {

            EpochContainer next = remainingItr.next();
            if (next.equals(container) && currentId > 0) {

                remainingItr.remove(); // Don't need to skip self as landing entry already added.
                continue;
            }

            if (next.getEpochId() >= container.getEpochId()
                    && next.getLeaderId() >= container.getLeaderId()) {
                ApplicationEntry entry = new ApplicationEntry(
                        new ApplicationEntry.ApplicationEntryId(
                                next.getEpochId(),
                                next.getLeaderId(),
                                0));

//                addEntryToLucene(writeEntryLuceneAdaptor, entry);
            } else {
                // Even though the data is removed, the landing entry has already been added.
                remainingItr.remove();
            }
        }

        return pendingUpdates;
    }


    private static LinkedList<EpochContainer> createClosedEpochHistoryList(int leaderId, int starting_Epoch, int count) {

        LinkedList<EpochContainer> historyList = new LinkedList<EpochContainer>();
        int maxValue = (starting_Epoch + count);
        
        for (int i = starting_Epoch; i < maxValue; i++) {
            historyList.add(new BaseEpochContainer(i, leaderId, closedContainerSize));
        }

        return historyList;
    }


    private static class LowestMissingTracker {

        private EpochContainer currentTrackingUpdate;
        private long currentTrackingId;

        public void setCurrentTrackingUpdate(EpochContainer update, long currentTrackingId) {
            this.currentTrackingUpdate = update;
            this.currentTrackingId = currentTrackingId;
        }

        public EpochContainer getCurrentTrackingUpdate() {
            return this.currentTrackingUpdate;
        }

        public ApplicationEntry.ApplicationEntryId getEntryBeingTracked() {

            ApplicationEntry.ApplicationEntryId entryId = null;

            if (currentTrackingUpdate != null) {

                entryId = new ApplicationEntry.ApplicationEntryId(currentTrackingUpdate.getEpochId(),
                        currentTrackingUpdate.getLeaderId(),
                        currentTrackingId);
            }

            return entryId;
        }
    }


    private static class EpochHistoryTracker {

        private LinkedList<EpochContainer> epochUpdateHistory = new LinkedList<EpochContainer>();

        public void setEpochHistory(Collection<EpochContainer> container) {
            epochUpdateHistory.addAll(container);
        }


        public void printEpochHistory(){
            System.out.println(epochUpdateHistory);
        }
        
        /**
         * Get the last update that has been added to the history tracker.
         * The application needs this information to know where to pull from.
         *
         * @return Epoch Update.
         */
        public EpochContainer getLastUpdate(){

            return !this.epochUpdateHistory.isEmpty()
                    ? this.epochUpdateHistory.getLast()
                    : null;
        }


        /**
         * Search for the update with the starting epochId.
         * Return the first known reference.
         *
         * @return Initial Epoch Update.
         */
        public EpochContainer getInitialEpochUpdate() {

            for (EpochContainer update : epochUpdateHistory) {
                if (update.getEpochId() == 0) {
                    return update;
                }
            }

            return null;
        }


        /**
         * Check for any updates to the entry matching the value provided by the
         * application.
         *
         * @param update Update to match against.
         * @return Updated Value.
         */
        public EpochContainer getSelfUpdate(EpochContainer update) {

            if (update == null) {
                return null;
            }

            for (EpochContainer epochUpdate : epochUpdateHistory) {
                if (epochUpdate.getEpochId() == update.getEpochId()
                        && epochUpdate.getLeaderId() == update.getLeaderId()) {

                    return epochUpdate;
                }
            }

            return null;
        }


        public List<EpochContainer> getNextUpdates(EpochContainer current, int limit) {

            List<EpochContainer> nextUpdates =
                    new ArrayList<EpochContainer>();

            if (current == null) {
                current = getInitialEpochUpdate();
            } else {
                current = getSelfUpdate(current);
            }

            if (current != null) 
            {

                
                
                if (!current.getEpochUpdateStatus()
                        .equals(EpochContainer.Status.ONGOING)) 
                {

                    int index = epochUpdateHistory.indexOf(current);
                    if (index != -1) 
                    {

                        ListIterator<EpochContainer> listIterator =
                                epochUpdateHistory.listIterator(index);

                        int count = 0;
                        while (listIterator.hasNext() && count < limit) {
                            nextUpdates.add(listIterator.next());
                            count++;
                        }
                    } 
                    
                    else 
                    {
                        throw new IllegalStateException("Unable to locate the resource ...");
                    }
                    
                }
                else{
                    nextUpdates.add(current);
                }
                
            }
            
            return nextUpdates;
        }
        
    }
}
