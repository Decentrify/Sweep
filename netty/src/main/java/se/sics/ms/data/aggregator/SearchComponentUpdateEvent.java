package se.sics.ms.data.aggregator;

import se.sics.ms.aggregator.data.ComponentUpdate;
import se.sics.ms.aggregator.type.ComponentUpdateEvent;

/**
 * Created by babbarshaer on 2015-03-20.
 */
public class SearchComponentUpdateEvent implements ComponentUpdateEvent{
    
    private final SearchComponentUpdate scu;
    
    public SearchComponentUpdateEvent(SearchComponentUpdate scu){
        this.scu = scu;
    }
    
    @Override
    public ComponentUpdate getComponentUpdate() {
        return this.scu;
    }
}
