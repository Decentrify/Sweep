package se.sics.ms.data.aggregator.design;

import se.sics.ktoolbox.aggregator.global.api.system.DesignInfoContainer;

import java.util.Collection;

/**
 * Container for the aggregated internal state being calculated from
 * multiple windows.
 * 
 * Created by babbarshaer on 2015-09-09.
 */
public class AggregatedInternalStateContainer extends DesignInfoContainer<AggregatedInternalState> {
    
    public AggregatedInternalStateContainer(Collection<AggregatedInternalState> processedWindows) {
        super(processedWindows);
    }
    
}
