package se.sics.ms.aggregator.data;

import se.sics.p2ptoolbox.aggregator.api.model.AggregatedStatePacket;

import java.util.Map;

/**
 * State Information for the defined components in the system.
 * *
 * Created by babbar on 2015-03-17.
 */

public class SweepAggregatedPacket implements AggregatedStatePacket{

    private final Map<Class, Map<Integer,ComponentUpdate>> componentDataMap;

    public SweepAggregatedPacket(Map<Class, Map<Integer, ComponentUpdate>> componentDataMap){
        this.componentDataMap = componentDataMap;
        
    }
    
    public Map<Class, Map<Integer, ComponentUpdate>> getComponentDataMap(){
        return this.componentDataMap;
    }

    @Override
    public String toString() {
        return "SweepAggregatedPacket{" +
                "componentDataMap=" + componentDataMap +
                '}';
    }
}

