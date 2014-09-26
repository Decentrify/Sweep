package se.sics.ms.gradient.misc;

import se.sics.ms.types.SearchDescriptor;

import java.util.Comparator;

/**
 * Compare nodes according to their utility.
 * Utility function: NatType (open/closed) - Number of index entries - Node id
 * @author: Steffen Grohsschmiedt
 */
public class UtilityComparator implements Comparator<SearchDescriptor> {

    @Override
    public int compare(SearchDescriptor o1, SearchDescriptor o2) {
        if (o1.getVodAddress().isOpen() != o2.getVodAddress().isOpen()) {
            if (o1.getVodAddress().isOpen())
                return 1;
            return -1;
        }

//        Add the number of index entries comparison to this also.
        if (o1.getNumberOfIndexEntries() != o2.getNumberOfIndexEntries()) {

            if (o1.getNumberOfIndexEntries() > o2.getNumberOfIndexEntries())
                return 1;
            return -1;
        }

        if (o1.getId() != o2.getId()) {

            if (o1.getId() > o2.getId())
                return -1;
            return 1;
        }

        return 0;
    }
}