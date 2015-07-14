package se.kth.ms.partitionaware.api.events;

import se.sics.kompics.KompicsEvent;
import se.sics.p2ptoolbox.util.network.impl.DecoratedAddress;

import java.util.Collection;

/**
 * Event depicting the case of possible network partition
 * in the system.
 *
 * Created by babbar on 2015-06-08.
 */
public class NPEvent implements KompicsEvent{

    public final Collection<DecoratedAddress> npNodes;

    public NPEvent(Collection<DecoratedAddress> nodes){
        this.npNodes = nodes;
    }

}
