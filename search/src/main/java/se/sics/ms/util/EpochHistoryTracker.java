package se.sics.ms.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.ms.types.EpochUpdate;

import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Stores and keep tracks of the epoch history.
 * The updates history is sorted based on the natural ordering of the 
 * Object.
 * 
 * TODO: Keep Track of lowest open epoch update
 * @author babbarshaer
 */
public class EpochHistoryTracker {
    
    private SortedSet<EpochUpdate> epochUpdateHistory;
    private static Logger logger = LoggerFactory.getLogger(EpochHistoryTracker.class);
    
    public EpochHistoryTracker(){
        logger.trace("Tracker Initialized .. ");
        epochUpdateHistory = new TreeSet<EpochUpdate>();
    }

    public void addEpochUpdate(EpochUpdate epochUpdate){
        this.epochUpdateHistory.add(epochUpdate);
    }

    public EpochUpdate getLastUpdate(){
        return this.epochUpdateHistory.last();
    }
    
}
