package se.sics.ms.gradient.misc;

import se.sics.ms.types.SearchDescriptor;

import java.util.Comparator;

/**
 * Compare nodes according to their utility.
 * Utility function: NatType (open/closed) - Leader Group Member - Partition Depth - Number of index entries - Node id
 *
 * @author: Steffen Grohsschmiedt
 */
public class UtilityComparator implements Comparator<SearchDescriptor> {

    @Override
    public int compare(SearchDescriptor o1, SearchDescriptor o2) {

        // NAT TYPE.
        if (o1.getVodAddress().isOpen() != o2.getVodAddress().isOpen()) {
            if (o1.getVodAddress().isOpen())
                return 1;
            return -1;
        }

        // LEADER GROUP MEMBERSHIP.
        if(o1.isLGMember() != o2.isLGMember()){
            if(o1.isLGMember())
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