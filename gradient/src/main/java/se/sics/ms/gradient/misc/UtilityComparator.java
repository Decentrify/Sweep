package se.sics.ms.gradient.misc;

import se.sics.ms.types.PeerDescriptor;

import java.util.Comparator;

/**
 * Compare nodes according to their utility.
 * Utility function: NatType (open/closed) - Leader Group Member - Partition Depth - Number of index entries - Node id
 *
 * @author: Steffen Grohsschmiedt
 */
public class UtilityComparator implements Comparator<PeerDescriptor> {

    @Override
    public int compare(PeerDescriptor o1, PeerDescriptor o2) {

//        // NAT TYPE.
//        if (o1.getVodAddress().isOpen() != o2.getVodAddress().isOpen()) {
//            if (o1.getVodAddress().isOpen())
//                return 1;
//            return -1;
//        }

        // LEADER GROUP MEMBERSHIP.
        if(o1.isLeaderGroupMember() != o2.isLeaderGroupMember()){
            if(o1.isLeaderGroupMember())
                return 1;
            return -1;
        }

        // PARTITIONING DEPTH.
        if(o1.getPartitioningDepth() != o2.getPartitioningDepth()) {
            if(o1.getPartitioningDepth() > o2.getPartitioningDepth())
                return 1;
            return -1;
        }

        // NUMBER OF INDEX ENTRIES.
        if (o1.getNumberOfIndexEntries() != o2.getNumberOfIndexEntries()) {

            if (o1.getNumberOfIndexEntries() > o2.getNumberOfIndexEntries())
                return 1;
            return -1;
        }

        // NODE ID.
        if (o1.getId() != o2.getId()) {

            if (o1.getId() > o2.getId())
                return -1;
            return 1;
        }

        return 0;
    }
}