package se.sics.util;

import se.sics.ms.types.SearchDescriptor;
import se.sics.p2ptoolbox.election.api.LCPeerView;

import java.util.Comparator;

/**
 * Comparator for the Leader Capable Peer View.
 *
 * Created by babbar on 2015-04-06.
 */
public class SimpleLCPViewComparator implements Comparator<LCPeerView>{

    SearchDescriptor s1;
    SearchDescriptor s2;
    @Override
    public int compare(LCPeerView o1, LCPeerView o2) {

        if(o1 instanceof SearchDescriptor && o2 instanceof SearchDescriptor){

            s1 = (SearchDescriptor)o1;
            s2 = (SearchDescriptor)o2;


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
