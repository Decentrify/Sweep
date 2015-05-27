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
     * Check if the epoch contains the leader unit in it.
     * The check that it performed here is exact equals check 
     * using the equals property of the  
     *  
     * @param leaderUnit unit
     * @return true / false
     */
    public boolean exactContainsCheck( LeaderUnit leaderUnit ){
        
        return this.leaderUnits.contains(leaderUnit);
    }


    /**
     * Check if the epoch contains the leader unit in it.
     * The matching criteria is loose in sense that it matches 
     * the leader and epoch information with the contained entries  
     * and avoids exact equals check.
     *  
     * @param leaderUnit unit
     * @return true / false
     */
    public boolean looseContainsCheck(LeaderUnit leaderUnit){
        
        for(LeaderUnit unit : leaderUnits){
            
            if(unit.getLeaderId() == leaderUnit.getLeaderId() 
                    && unit.getEpochId() == leaderUnit.getEpochId()){
                
                return true;
            }
        }

        return false;
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

    /**
     * Based on the leader unit list that is contained in the epoch
     * calculate the last added leader unit entry.
     *
     * @return Leader Unit.
     */
    public LeaderUnit getLastUpdate() {

        return ( !leaderUnits.isEmpty() )
                ? leaderUnits.get(leaderUnits.size()-1)
                : null;
    }

    /**
     * As the leader unit information changes in terms of it getting closed,
     * this information needs to be propagated down in the gradient,
     * so based on the receiving leader unit check a similar one on the epoch and the
     * leader id and therefore return the matching value.
     *
     * @param base
     * @return
     */
    public LeaderUnit getLooseLeaderUnit( LeaderUnit base){

        for(LeaderUnit unit : leaderUnits){

            if(unit.getEpochId() == base.getEpochId()
                    && unit.getLeaderId() == base.getLeaderId()) {
                return unit;
            }
        }

        return null;
    }



    
    public LeaderUnit getLeaderUnit(LeaderUnit leaderUnit){
        
        int index;
        LeaderUnit lu = null;
        index = this.leaderUnits.indexOf(leaderUnit);
        
        if(index != -1){
            lu = this.leaderUnits.get(index);
        }
        
        return lu;
    }


    public LeaderUnit getFirstUnit() {

        return !this.leaderUnits.isEmpty()
                ? this.leaderUnits.get(0)
                : null;
    }
}
