package se.sics.ms.util;

import se.sics.ms.common.RoutingTableContainer;
import se.sics.ms.types.PeerDescriptor;

import java.util.Comparator;

/**
 * Simple Collection for the comparators acting on {@link se.sics.ms.types.PeerDescriptor}.
 * Created by babbar on 2015-03-24.
 */
public class ComparatorCollection {

    /**
     * Comparator based on the age of the descriptors.
     * It transfers the comparison to the native descriptors in case of ties.
     */
    public static class AgeComparator implements Comparator<RoutingTableContainer>{

        @Override
        public int compare(RoutingTableContainer o1, RoutingTableContainer o2) {

            if(o1.getAge() < o2.getAge()){
                return 1;
            }

            else if(o1.getAge() > o2.getAge()){
                return -1;
            }

            PeerDescriptor sd1 = o1.getContent();
            PeerDescriptor sd2 = o2.getContent();

            // Comparing on the peer views to break ties.
            return sd1.compareTo(sd2);
        }
    }


    /**
     * Comparator used to sort the collection which is opposite as compared to the {@link se.sics.ms.util.ComparatorCollection.AgeComparator}.
     */
    public static class InvertedAgeComparator implements Comparator<RoutingTableContainer>{

        @Override
        public int compare(RoutingTableContainer o1, RoutingTableContainer o2) {

            if(o1.getAge() < o2.getAge()){
                return -1;
            }

            else if(o1.getAge() > o2.getAge()){
                return 1;
            }

            PeerDescriptor sd1 = o1.getContent();
            PeerDescriptor sd2 = o2.getContent();

            // Comparing on the peer views to break ties.
            // Invert the result before sending.
            return (-1 * (sd1.compareTo(sd2)));
        }
    }


    /**
     * Comparator used to sort a collection
     * of the IdScorePair Object types.  
     */
    public static class IdScorePairComparator implements Comparator<IdScorePair>{

        @Override
        public int compare(IdScorePair o1, IdScorePair o2) {
            
            int result = 0;
            result = Float.compare(o1.getScore(), o2.getScore());
            
            if(result == 0){
                result = o1.getEntryId().compareTo(o2.getEntryId());
            }
            
            return result;
        }
    }


}
