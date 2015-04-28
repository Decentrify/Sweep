package se.sics.ms.data.aggregator;

import se.sics.ms.aggregator.data.ComponentUpdate;

/**
 * Update to the Local Aggregator from the Leader Component.
 * Created by babbarshaer on 2015-03-20.
 */
public class ElectionLeaderComponentUpdate implements ComponentUpdate {
    
    private final boolean isLeader;
    private final int componentOverlay;
    
    
    public ElectionLeaderComponentUpdate(boolean isLeader, int componentOverlay){
        this.isLeader = isLeader;
        this.componentOverlay = componentOverlay;
    }
    
    
    public boolean isLeader(){
        return this.isLeader;
    }
    
    
    @Override
    public int getComponentOverlay() {
        return this.componentOverlay;
    }
}
