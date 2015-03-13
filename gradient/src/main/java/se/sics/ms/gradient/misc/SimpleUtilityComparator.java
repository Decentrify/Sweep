package se.sics.ms.gradient.misc;

import se.sics.ms.types.SearchDescriptor;
import se.sics.p2ptoolbox.croupier.api.util.PeerView;

import java.util.Comparator;

/**
 * Created by babbarshaer on 2015-03-06.
 */
public class SimpleUtilityComparator implements Comparator<PeerView>{
    
    SearchDescriptor s1;
    SearchDescriptor s2;
    
    @Override
    public int compare(PeerView o1, PeerView o2) {

        if(o1 instanceof SearchDescriptor && o2 instanceof SearchDescriptor){
            
            s1 = (SearchDescriptor)o1;
            s2 = (SearchDescriptor)o2;

            // NAT TYPE.
            if (s1.getVodAddress().isOpen() != s2.getVodAddress().isOpen()) {
                if (s1.getVodAddress().isOpen())
                    return 1;
                return -1;
            }

            // PARTITIONING DEPTH.
            if(s1.getReceivedPartitionDepth() != s2.getReceivedPartitionDepth()) {
                if(s1.getReceivedPartitionDepth() > s2.getReceivedPartitionDepth())
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
