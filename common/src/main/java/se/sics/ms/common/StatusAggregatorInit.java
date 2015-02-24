package se.sics.ms.common;

import se.sics.gvod.net.VodAddress;
import se.sics.kompics.Init;

/**
 * Initialization Class for the Status Aggregator.
 * 
 * Created by babbarshaer on 2015-02-19.
 */
public class StatusAggregatorInit extends Init<StatusAggregator>{

    private final VodAddress mainSimAddress;
    private final VodAddress self;
    private final int timeout;
    
    public StatusAggregatorInit(VodAddress mainSimAddress, VodAddress self, int timeout){
        this.mainSimAddress = mainSimAddress;
        this.self = self;
        this.timeout = timeout;
    }

    public VodAddress getMainSimAddress() {
        return mainSimAddress;
    }

    public VodAddress getSelf() {
        return self;
    }

    public int getTimeout(){
        return this.timeout;
    }
}
