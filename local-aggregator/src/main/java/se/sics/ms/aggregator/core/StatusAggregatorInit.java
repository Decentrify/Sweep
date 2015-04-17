package se.sics.ms.aggregator.core;

import se.sics.gvod.net.VodAddress;
import se.sics.kompics.Init;
import se.sics.p2ptoolbox.util.network.impl.DecoratedAddress;

/**
 * Initialization Class for the Status Aggregator.
 * 
 * Created by babbarshaer on 2015-02-19.
 */
public class StatusAggregatorInit extends Init<StatusAggregator>{

    private final DecoratedAddress mainSimAddress;
    private final DecoratedAddress self;
    private final int timeout;
    
    public StatusAggregatorInit(DecoratedAddress mainSimAddress, DecoratedAddress self, int timeout){
        this.mainSimAddress = mainSimAddress;
        this.self = self;
        this.timeout = timeout;
    }

    public DecoratedAddress getMainSimAddress() {
        return mainSimAddress;
    }

    public DecoratedAddress getSelf() {
        return self;
    }

    public int getTimeout(){
        return this.timeout;
    }
}
