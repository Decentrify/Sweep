
package se.sics.ms.data.aggregator.design;

import se.sics.ktoolbox.aggregator.server.util.DesignInfoContainer;

import java.util.Collection;

/**
 * Container for holding the collection of avg search responses.
 *
 * Created by babbar on 2015-09-06.
 */
public class AvgSearchRespContainer extends DesignInfoContainer<AvgSearchResponse> {

    public AvgSearchRespContainer(Collection<AvgSearchResponse> processedWindows) {
        super(processedWindows);
    }
}
