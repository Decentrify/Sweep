package se.kth.ms.partitionaware.api.events;

import se.sics.kompics.KompicsEvent;
import se.sics.ms.types.SearchDescriptor;

/**
 * Main Update Event to inform the PAG about the 
 * possible update of the self descriptor.
 * 
 * Created by babbarshaer on 2015-06-03.
 */
public class PALUpdate implements KompicsEvent {
    
    private final SearchDescriptor selfView;
    
    public PALUpdate(SearchDescriptor selfView){
        this.selfView = selfView;
    }

    public SearchDescriptor getSelfView() {
        return selfView;
    }
}
