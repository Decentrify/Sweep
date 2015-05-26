package se.sics.ms.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.ms.types.LeaderUnit;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 *
 * Created by babbarshaer on 2015-05-25.
 */
public class Epoch {
    
    private long epochId;
    private List<LeaderUnit> leaderUnits;
    public static Logger logger = LoggerFactory.getLogger(TimeLine.class);
    
    public Epoch(long epochId){
        
        this.epochId = epochId;
        this.leaderUnits = new ArrayList<LeaderUnit>();
    }


    /**
     * Store a leader unit in the epoch. 
     * Check for any updates to the existing leader units  
     * and also sort the collection once the leader unit is added.
     *
     * @param leaderUnit LeaderUnit.
     */
    public void addLeaderUnit (LeaderUnit leaderUnit){
        
        if(leaderUnit.getEpochId() != epochId){
            logger.error("Trying to store leader unit with epochId : {} in Epoch structure for epochId: {}", leaderUnit.getEpochId(), epochId);
            return;
        }
        
        for(int i =0 ; i < leaderUnits.size() ; i ++){
            LeaderUnit lu = leaderUnits.get(i);
            
            if(lu.getLeaderId() == leaderUnit.getLeaderId()) {
                leaderUnit.setEntryPullStatus( lu.getEntryPullStatus());
                leaderUnits.set(i, leaderUnit);
                return;
            }
        }
        
        leaderUnits.add(leaderUnit);
        Collections.sort(leaderUnits, new GenericECComparator());
    }


    public long getEpochId() {
        return epochId;
    }

    public List<LeaderUnit> getLeaderUnits() {
        return leaderUnits;
    }
}
