package se.kth.ms.api.events;

import se.sics.kompics.KompicsEvent;
import se.sics.ms.types.SearchDescriptor;

/**
 * Main Update Event to inform the PAG about the 
 * possible update of the self descriptor.
 * 
 * Created by babbarshaer on 2015-06-03.
 */
public class PAGUpdate implements KompicsEvent {
    
    private final SearchDescriptor selfView;
    
    public PAGUpdate(SearchDescriptor selfView){
        this.selfView = selfView;
    }

    public SearchDescriptor getSelfView() {
        return selfView;
    }
}
