package se.sics.ms.data.aggregator.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.ktoolbox.aggregator.common.PacketInfo;
import se.sics.ktoolbox.aggregator.server.api.system.DesignInfoContainer;
import se.sics.ktoolbox.aggregator.server.api.system.DesignProcessor;
import se.sics.ms.data.aggregator.design.AggregatedInternalState;
import se.sics.ms.data.aggregator.design.AggregatedInternalStateContainer;
import se.sics.ms.data.aggregator.packets.InternalStatePacket;
import se.sics.p2ptoolbox.util.network.impl.BasicAddress;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Design Processor which will be
 * Created by babbarshaer on 2015-09-09.
 */
public class InternalStateDesignProcessor implements DesignProcessor<InternalStatePacket, AggregatedInternalState> {
    
    Logger logger = LoggerFactory.getLogger(InternalStateDesignProcessor.class);
    
    @Override
    public DesignInfoContainer<AggregatedInternalState> process(Collection<Map<BasicAddress, List<PacketInfo>>> windows) {
        
        logger.debug("Initiating with the processing of the internal state packets per window.");
        Collection<AggregatedInternalState> processedWindows = new ArrayList<AggregatedInternalState>();
        
        for(Map<BasicAddress, List<PacketInfo>> window : windows){
            
            Collection<InternalStatePacket> statePackets = new ArrayList<InternalStatePacket>();
            for(Map.Entry<BasicAddress, List<PacketInfo>> entry : window.entrySet()){
                
                for(PacketInfo packet :entry.getValue()){
                    
                    if(packet instanceof InternalStatePacket){
                        statePackets.add((InternalStatePacket) packet);
                    }
                }
            }
            
            processedWindows.add(new AggregatedInternalState(statePackets));
        }
        
        return new AggregatedInternalStateContainer(processedWindows);
    }

    @Override
    public void cleanState() {
        logger.debug("Invoked by the visualizer to clean some internal state.");
    }
}
