package se.sics.ms.gradient.events;

import se.sics.kompics.KompicsEvent;

import java.util.List;
import se.sics.ktoolbox.util.network.KAddress;

/**
 * Event depicting the case of possible network partition
 * in the system.
 *
 * Created by babbar on 2015-06-08.
 */
public class NPEvent implements KompicsEvent{

    public final List<KAddress> npNodes;

    public NPEvent(List<KAddress> nodes){
        this.npNodes = nodes;
    }

}
