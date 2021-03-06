package se.sics.ms.util;

import se.sics.ms.types.PeerDescriptor;

import java.util.Comparator;
import se.sics.ktoolbox.election.util.LCPeerView;

/**
 * Comparator for the Leader Capable Peer View.
 *
 * Created by babbar on 2015-04-06.
 */
public class SimpleLCPViewComparator implements Comparator<LCPeerView>{

    PeerDescriptor s1;
    PeerDescriptor s2;

    @Override
    public int compare(LCPeerView o1, LCPeerView o2) {

        if(o1 instanceof PeerDescriptor && o2 instanceof PeerDescriptor){

            s1 = (PeerDescriptor)o1;
            s2 = (PeerDescriptor)o2;


            // LEADER GROUP MEMBERSHIP.
            if(s1.isLeaderGroupMember() != s2.isLeaderGroupMember()){
                if(s1.isLeaderGroupMember())
                    return 1;
                return -1;
            }

            // PARTITIONING DEPTH.
            if(s1.getPartitioningDepth() != s2.getPartitioningDepth()) {
                if(s1.getPartitioningDepth() > s2.getPartitioningDepth())
                    return 1;
                return -1;
            }

            // NUMBER OF INDEX ENTRIES.
            if (s1.getNumberOfIndexEntries() != s2.getNumberOfIndexEntries()) {

                if (s1.getNumberOfIndexEntries() > s2.getNumberOfIndexEntries())
                    return 1;
                return -1;
            }

            // NODE ID.
            if (s1.getId() != s2.getId()) {

                if (s1.getId() > s2.getId())
                    return -1;
                return 1;
            }

            return 0;
        }

        throw new ClassCastException("Comparator's Not Valid for the Object.");
    }
}
