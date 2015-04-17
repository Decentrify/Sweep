package se.sics.ms.util;

import se.sics.ms.common.RoutingTableContainer;
import se.sics.ms.types.SearchDescriptor;

import java.util.Comparator;

/**
 * Simple Collection for the comparators acting on {@link SearchDescriptor}.
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

            SearchDescriptor sd1 = o1.getContent();
            SearchDescriptor sd2 = o2.getContent();

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

            SearchDescriptor sd1 = o1.getContent();
            SearchDescriptor sd2 = o2.getContent();

            // Comparing on the peer views to break ties.
            // Invert the result before sending.
            return (-1 * (sd1.compareTo(sd2)));
        }
    }


}
