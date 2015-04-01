package se.sics.p2ptoolbox.election.example.main;

import se.sics.p2ptoolbox.election.api.LCPeerView;

import java.util.Comparator;

/**
 * Leader Capable Comparator.
 *
 * Created by babbar on 2015-04-01.
 */
public class LCPComparator implements Comparator<LCPeerView>{

    @Override
    public int compare(LCPeerView o1, LCPeerView o2) {

        if(o1 == null || o2 == null){
            throw new IllegalArgumentException("Can't compare null values.");
        }

        if(o1 instanceof LeaderDescriptor && o2 instanceof LeaderDescriptor){

            LeaderDescriptor ld1 = (LeaderDescriptor)o1;
            LeaderDescriptor ld2 = (LeaderDescriptor)o2;

            return ld1.compareTo(ld2);

        }

        else{
            throw new IllegalArgumentException("Types for comparison are not application speific.");
        }
    }
}
