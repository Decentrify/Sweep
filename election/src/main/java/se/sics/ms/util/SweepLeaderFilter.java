package se.sics.ms.util;

import se.sics.ms.types.PeerDescriptor;

import java.util.Collection;
import se.sics.ktoolbox.election.util.LCPeerView;
import se.sics.ktoolbox.election.util.LeaderFilter;
import se.sics.ktoolbox.util.network.KAddress;

/**
 * Filter used by the leader election protocol.
 *
 * Created by babbar on 2015-04-06.
 */
public class SweepLeaderFilter implements LeaderFilter{


    @Override
    public boolean initiateLeadership(Collection<KAddress> collection) {
        return false;
    }

    @Override
    public boolean terminateLeader(LCPeerView old, LCPeerView updated) {

        PeerDescriptor previous;
        PeerDescriptor current;

        if( !(old instanceof PeerDescriptor) || !(updated instanceof PeerDescriptor)){
            throw new IllegalArgumentException("Unknown types of arguments.");
        }
        
        previous = (PeerDescriptor)old;
        current = (PeerDescriptor)updated;
        
        // Return true in event of increase in partitioning depth.
        return (current.getPartitioningDepth() > previous.getPartitioningDepth());
    }
}
