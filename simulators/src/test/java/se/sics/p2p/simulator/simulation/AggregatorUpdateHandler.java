package se.sics.p2p.simulator.simulation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.KompicsEvent;
import se.sics.ms.data.ComponentStatus;
import se.sics.ms.data.GradientStatusData;
import se.sics.ms.data.SearchStatusData;
import se.sics.ms.types.AggregatorUpdateMsg;
import se.sics.p2ptoolbox.simulator.SimulationContext;
import se.sics.p2ptoolbox.simulator.SystemStatusHandler;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by babbarshaer on 2015-02-20.
 */
public class AggregatorUpdateHandler implements SystemStatusHandler{
    
    Logger logger = LoggerFactory.getLogger(AggregatorUpdateHandler.class);
    private Map<String, ComponentStatus> componentStatusMap;
    
    
    @Override
    public Class getStatusMsgType() {
        return AggregatorUpdateMsg.class;
    }

    @Override
    public void handle(KompicsEvent msg, SimulationContext context) {
        logger.info(" Received periodic update from the aggregator component.");
        
        if(msg instanceof AggregatorUpdateMsg){
            
            // Typecast message and create a copy of map.
            AggregatorUpdateMsg updateMsg = (AggregatorUpdateMsg)msg;
            componentStatusMap = new HashMap<String, ComponentStatus>(updateMsg.getComponentStatusMap());

            // TODO: Not sure if below is a good design decision.
            // Start fetching the component data.
            SearchStatusData searchStatusData = (SearchStatusData) componentStatusMap.get(SearchStatusData.SEARCH_KEY);
            
            //Use SimulationContext.
            
        }
        
        else{
            logger.warn("Unknown aggregator message type.");
        }
    }
}
