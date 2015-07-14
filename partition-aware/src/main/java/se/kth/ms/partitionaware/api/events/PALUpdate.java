package se.kth.ms.partitionaware.api.events;

import se.sics.kompics.KompicsEvent;
import se.sics.ms.types.PeerDescriptor;

/**
 * Main Update Event to inform the PAG about the 
 * possible update of the self descriptor.
 * 
 * Created by babbarshaer on 2015-06-03.
 */
public class PALUpdate implements KompicsEvent {
    
    private final PeerDescriptor selfView;
    
    public PALUpdate(PeerDescriptor selfView){
        this.selfView = selfView;
    }

    public PeerDescriptor getSelfView() {
        return selfView;
    }
}
