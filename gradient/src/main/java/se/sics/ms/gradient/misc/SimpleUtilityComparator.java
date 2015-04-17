package se.sics.ms.gradient.misc;

import se.sics.ms.types.SearchDescriptor;

import java.util.Comparator;

/**
 * Comparator used in the gradient protocol.
 *
 * Created by babbarshaer on 2015-03-06.
 */
public class SimpleUtilityComparator implements Comparator<SearchDescriptor>{

        @Override
        public int compare(SearchDescriptor s1, SearchDescriptor s2) {

                // NAT TYPE. ASSUME ALL OPEN.
                /*if (s1.getVodAddress().isOpen() != s2.getVodAddress().isOpen()) {
                    if (s1.getVodAddress().isOpen())
                        return 1;
                    return -1;
                }*/

                // LEADER GROUP MEMBERSHIP.
                if(s1.isLGMember() != s2.isLGMember()){
                    if(s1.isLGMember())
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
}
