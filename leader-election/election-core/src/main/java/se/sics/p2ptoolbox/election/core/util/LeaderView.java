package se.sics.p2ptoolbox.election.core.util;

import se.sics.gvod.croupier.Croupier;
import se.sics.p2ptoolbox.croupier.api.util.CroupierPeerView;
import se.sics.p2ptoolbox.croupier.api.util.PeerView;

import java.util.*;

/**
 * This is mainly a helper class for the Leader Election Algorithm.
 *
 * Created by babbarshaer on 2015-03-28.
 */
public class LeaderView {
    
    private Comparator<CroupierPeerView> utilityComparator;
    private Comparator<PeerView> pvUtilityComparator;
    private SortedSet<CroupierPeerView> higherUtilityNodes;
    private SortedSet<CroupierPeerView> lowerUtilityNodes;

    
    
    public LeaderView(Comparator<PeerView> pvUtilityComparator){
        
        this.pvUtilityComparator = pvUtilityComparator;
        this.utilityComparator = new UtilityComparator();
        
        lowerUtilityNodes = new TreeSet<CroupierPeerView>(utilityComparator);
        higherUtilityNodes = new TreeSet<CroupierPeerView>(utilityComparator);
    }


    
    
    
    /**
     * Based on the collection provided, calculate the nodes that have a higher
     * utility than the self view.
     *
     * @param cpvCollection View Collection
     * @param selfCPV Self View
     * @return Higher Utility Sorted Set 
     */
    public SortedSet<CroupierPeerView> getHigherUtilityNodes(Collection<CroupierPeerView> cpvCollection , CroupierPeerView selfCPV){
        
        higherUtilityNodes.clear();
        higherUtilityNodes.addAll(cpvCollection);
        
        return higherUtilityNodes.tailSet(selfCPV);
    }


    /**
     * Based on the collection provided, calculate the nodes that have a lower
     * utility than the self view.
     *
     * @param cpvCollection View Collection
     * @param selfCPV Self View
     * @return Lower Utility Sorted Set
     */
    public SortedSet<CroupierPeerView> getLowerUtilityNodes(Collection<CroupierPeerView> cpvCollection, CroupierPeerView selfCPV){
        
        lowerUtilityNodes.clear();
        lowerUtilityNodes.addAll(cpvCollection);
        
        return lowerUtilityNodes.headSet(selfCPV);
    }
    
    

    

    private class UtilityComparator implements  Comparator<CroupierPeerView>{

        @Override
        public int compare(CroupierPeerView o1, CroupierPeerView o2) {

            double compareToValue = Math.signum(pvUtilityComparator.compare(o1.pv, o2.pv));
            if (compareToValue == 0) {
                //should use CroupierPeerView compareTo to be equal consistent
                return (int) compareToValue;
            }
            return (int) compareToValue;
        }
    }
    
}
