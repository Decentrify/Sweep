
package se.sics.ms.main;

import se.sics.ktoolbox.aggregator.server.event.AggregatedInfo;

/**
 * The helper interface exposes methods that will be used by the
 * aggregator host component.
 *
 * Created by babbar on 2015-09-19.
 */
public interface AggregatorCompHelper {


    /**
     * Filter the information according to the requirement and then construct the
     * updated aggregated information.
     *
     * @param originalInfo original information.
     * @return filtered information
     */
    public AggregatedInfo filter (AggregatedInfo originalInfo);
}
