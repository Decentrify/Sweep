

package se.sics.util;

import se.sics.p2ptoolbox.election.api.LCPeerView;
import se.sics.p2ptoolbox.election.api.LEContainer;
import se.sics.p2ptoolbox.util.network.impl.DecoratedAddress;

import java.util.Comparator;

/**
 * Comparator for the LEContainer.
 *
 * Created by babbar on 2015-08-11.
 */
public class LEContainerComparator implements Comparator<LEContainer>{

    private Comparator<LCPeerView> lcPeerViewComparator;
    private Comparator<DecoratedAddress> addressComparator;

    public LEContainerComparator(Comparator<LCPeerView> lcPeerViewComparator, Comparator<DecoratedAddress> addressComparator){
        this.lcPeerViewComparator = lcPeerViewComparator;
        this.addressComparator = addressComparator;
    }

    @Override
    public int compare(LEContainer o1, LEContainer o2) {

        if (o1 == null || o2 == null) {
            throw new IllegalArgumentException("Can't compare null values");
        }

        int result ;

        LCPeerView view1 = o1.getLCPeerView();
        LCPeerView view2 = o2.getLCPeerView();


        result = lcPeerViewComparator.compare(view1, view2);

        if(result != 0)
            return result;

//      Tie breaker.
        return addressComparator.compare(o1.getSource(), o2.getSource());
    }
}
