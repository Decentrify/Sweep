package se.kth.ms.partitionaware.api.events;

import se.sics.kompics.KompicsEvent;

import java.util.Collection;
import se.sics.ktoolbox.util.network.KAddress;

/**
 * Event depicting the case of possible network partition
 * in the system.
 *
 * Created by babbar on 2015-06-08.
 */
public class NPEvent implements KompicsEvent{

    public final Collection<KAddress> npNodes;

    public NPEvent(Collection<KAddress> nodes){
        this.npNodes = nodes;
    }

}
