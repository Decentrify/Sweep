package se.sics.ms.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.ms.search.NPAwareSearch;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Tracker for entry addition rounds in the system.
 * Keep track of all the addition rounds in the system.
 * 
 * Created by babbarshaer on 2015-04-13.
 */
public class MultipleEntryAdditionTracker {
    
    private int maxCount =0;
    private Map<UUID, EntryAdditionRoundInfo> entryAdditionRoundInfoMap;
    Logger logger  = LoggerFactory.getLogger(NPAwareSearch.class);
    
    public MultipleEntryAdditionTracker(int maxCount){
        this.maxCount = maxCount;
        this.entryAdditionRoundInfoMap = new HashMap<UUID, EntryAdditionRoundInfo>();
    }

    public void startTracking(UUID additionRoundId, EntryAdditionRoundInfo additionRoundInfo){
        
        if(entryAdditionRoundInfoMap!= null && !entryAdditionRoundInfoMap.containsKey(additionRoundId)){
            entryAdditionRoundInfoMap.put(additionRoundId, additionRoundInfo);
        }
        else{
            logger.warn("Round: {} already being tracked.", additionRoundId);
        }
    }

    /**
     * Based on the entry addition round id, fetch the entry addition round information
     * stored locally in the map.
     *
     * @param entryAdditionRoundId addition round id.
     * @return Round Info.
     */
    public EntryAdditionRoundInfo getEntryAdditionRoundInfo(UUID entryAdditionRoundId){
        return this.entryAdditionRoundInfoMap.get(entryAdditionRoundId);
    }
    
    /**
     * Reset the tracker information the specified entry addition round.
     * @param entryAdditionRound addition round.
     */
    public void resetTracker(UUID entryAdditionRound){
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
