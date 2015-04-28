package se.sics.ms.data.aggregator;

import se.sics.ms.aggregator.data.ComponentUpdate;
import se.sics.ms.aggregator.type.ComponentUpdateEvent;

/**
 * Event sent by the leader component to indicate the state information.
 *
 * Created by babbarshaer on 2015-03-20.
 */
public class ElectionLeaderUpdateEvent implements ComponentUpdateEvent{
    
    private final ElectionLeaderComponentUpdate lcu;
    
    public ElectionLeaderUpdateEvent(ElectionLeaderComponentUpdate lcu){
            this.lcu = lcu;
    }

    public ElectionLeaderComponentUpdate getElectionLeaderComponentUpdate(){
        return this.lcu;
    }
    
    @Override
    public ComponentUpdate getComponentUpdate() {
        return this.lcu;
    }
}
