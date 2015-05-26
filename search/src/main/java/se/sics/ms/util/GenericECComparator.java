package se.sics.ms.util;

import se.sics.ms.types.LeaderUnit;

import java.util.Comparator;

/**
 *  
 * Created by babbarshaer on 2015-05-20.
 */
public class GenericECComparator implements Comparator<LeaderUnit> {
    
    @Override
    public int compare(LeaderUnit o1, LeaderUnit o2) {
        
        if(o1 == o2){
            return 0;
        }
        
        int epochComparison  = Long.valueOf(o1.getEpochId()).compareTo(o2.getEpochId());
        if(epochComparison != 0){
            return epochComparison;
        }
        
        int leaderComparison = Integer.valueOf(o1.getLeaderId()).compareTo(o2.getLeaderId());
        if(leaderComparison != 0){
            return leaderComparison;
        }
        
        return Long.valueOf(o1.getNumEntries()).compareTo(o2.getNumEntries());
    }
}
