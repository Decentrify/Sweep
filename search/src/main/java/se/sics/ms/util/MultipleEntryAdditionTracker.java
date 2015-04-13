package se.sics.ms.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.timer.TimeoutId;
import se.sics.ms.messages.ReplicationPrepareCommitMessage;
import se.sics.ms.search.Search;

import java.util.HashMap;
import java.util.Map;

/**
 * Tracker for entry addition rounds in the system.
 * Keep track of all the addition rounds in the system.
 * 
 * Created by babbarshaer on 2015-04-13.
 */
public class MultipleEntryAdditionTracker {
    
    private int maxCount =0;
    private Map<TimeoutId, EntryAdditionRoundInfo> entryAdditionRoundInfoMap;
    Logger logger  = LoggerFactory.getLogger(Search.class);
    
    public MultipleEntryAdditionTracker(int maxCount){
        this.maxCount = maxCount;
        this.entryAdditionRoundInfoMap = new HashMap<TimeoutId, EntryAdditionRoundInfo>();
    }

    public void startTracking(TimeoutId additionRoundId, EntryAdditionRoundInfo additionRoundInfo){
        
        if(entryAdditionRoundInfoMap!= null && !entryAdditionRoundInfoMap.containsKey(additionRoundId)){
            entryAdditionRoundInfoMap.put(additionRoundId, additionRoundInfo);
        }
        else{
            logger.warn("Round: {} already being tracked.", additionRoundId);
        }
    }

    /**
     * Convenience Method for collating and adding promise responses.
     * @param response Promise Response.
     */
    public void addEntryAddPromiseResponse(ReplicationPrepareCommitMessage.Response response){

        TimeoutId roundId = response.getTimeoutId();
        if(this.entryAdditionRoundInfoMap != null && this.entryAdditionRoundInfoMap.containsKey(roundId)){
            this.entryAdditionRoundInfoMap.get(roundId).addEntryAddPromiseResponse(response);
        }
    }

    public EntryAdditionRoundInfo getEntryAdditionRoundInfo(TimeoutId entryAdditionRoundId){
        return this.entryAdditionRoundInfoMap.get(entryAdditionRoundId);
    }
    
    /**
     * Reset the tracker information the specified entry addition round.
     * @param entryAdditionRound addition round.
     */
    public void resetTracker(TimeoutId entryAdditionRound){
        if(entryAdditionRoundInfoMap != null){
            entryAdditionRoundInfoMap.remove(entryAdditionRound);
        }
    }
    
    /**
     * Can the helper class keep track of another entry addition round
     * @return true if can keep track.
     */
    public boolean canTrack(){
        return entryAdditionRoundInfoMap.size() < maxCount;
    }
    
    @Override
    public String toString(){
        
        String result = "";
        for(EntryAdditionRoundInfo info : entryAdditionRoundInfoMap.values()){
            result +=info.toString();
        }
        return result;
    }
}
