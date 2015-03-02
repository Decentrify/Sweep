package se.sics.ms.handlers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.KompicsEvent;
import se.sics.ms.common.ComponentUpdateEnum;
import se.sics.ms.data.ComponentUpdate;
import se.sics.ms.data.SearchComponentUpdate;
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
    private Map<String, ComponentUpdate> componentStatusMap;
    private long currentIndexEntryCount;
    private int currentPartitioningDepth;
    
    
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
            componentStatusMap = new HashMap<String, ComponentUpdate>(updateMsg.getComponentStatusMap());
            
            // check search data.
            ComponentUpdate update = componentStatusMap.get(ComponentUpdateEnum.SEARCH.getName());
            SearchComponentUpdate searchComponentUpdate = null;
            if(update != null){
                
                if (ComponentUpdateEnum.SEARCH.getUpdateType().isInstance(update)){
                    
                    searchComponentUpdate = (SearchComponentUpdate)update;
                    currentIndexEntryCount = searchComponentUpdate.getSearchDescriptor().getNumberOfIndexEntries();
                    currentPartitioningDepth = searchComponentUpdate.getSearchDescriptor().getReceivedPartitionDepth();
                    logger.warn("Aggregator Update: Index Entry : {}, Partitioning Depth: {}", currentIndexEntryCount,currentPartitioningDepth );
                    
                }
            }
        }
        
        else{
            logger.warn("Unknown aggregator message type.");
        }
    }
}
