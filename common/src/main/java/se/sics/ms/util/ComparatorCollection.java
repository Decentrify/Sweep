package se.sics.ms.util;

import se.sics.ms.types.SearchDescriptor;
import se.sics.p2ptoolbox.croupier.api.util.CroupierPeerView;

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
    public static class AgeComparator implements Comparator<CroupierPeerView>{

        @Override
        public int compare(CroupierPeerView o1, CroupierPeerView o2) {


            if( !(o1.pv instanceof  SearchDescriptor) || !(o2.pv instanceof SearchDescriptor) ){
                throw new IllegalArgumentException("Unrecognized Peer Views to compare.");
            }

            if(o1.getAge() < o2.getAge()){
                return 1;
            }

            else if(o1.getAge() > o2.getAge()){
                return -1;
            }

            SearchDescriptor sd1 = (SearchDescriptor)o1.pv;
            SearchDescriptor sd2 = (SearchDescriptor)o2.pv;

            // Comparing on the peer views to break ties.
            return sd1.compareTo(sd2);
        }
    }


    /**
     * Comparator used to sort the collection which is opposite as compared to the {@link se.sics.ms.util.ComparatorCollection.AgeComparator}.
     */
    public static class InvertedAgeComparator implements Comparator<CroupierPeerView>{

        @Override
        public int compare(CroupierPeerView o1, CroupierPeerView o2) {


            if( !(o1.pv instanceof  SearchDescriptor) || !(o2.pv instanceof SearchDescriptor) ){
                throw new IllegalArgumentException("Unrecognized Peer Views to compare.");
            }

            if(o1.getAge() < o2.getAge()){
                return -1;
            }

            else if(o1.getAge() > o2.getAge()){
                return 1;
            }

            SearchDescriptor sd1 = (SearchDescriptor)o1.pv;
            SearchDescriptor sd2 = (SearchDescriptor)o2.pv;

            // Comparing on the peer views to break ties.
            // Invert the result before sending.
            return (-1 * (sd1.compareTo(sd2)));
        }
    }


}
