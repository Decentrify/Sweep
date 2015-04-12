package se.sics.util;

import se.sics.ms.types.SearchDescriptor;
import se.sics.p2ptoolbox.election.api.LCPeerView;
import se.sics.p2ptoolbox.election.core.util.LeaderFilter;

/**
 * Filter used by the leader election protocol.
 *
 * Created by babbar on 2015-04-06.
 */
public class SweepLeaderFilter implements LeaderFilter{

    @Override
    public boolean terminateLeader(LCPeerView old, LCPeerView updated) {

        SearchDescriptor previous;
        SearchDescriptor current;

        if( !(old instanceof SearchDescriptor) || !(updated instanceof SearchDescriptor)){
            throw new IllegalArgumentException("Unknown types of arguments.");
        }
        
        previous = (SearchDescriptor)old;
        current = (SearchDescriptor)updated;
        
        // Return true in event of increase in partitioning depth.
        return (current.getPartitioningDepth() > previous.getPartitioningDepth());
    }
}
