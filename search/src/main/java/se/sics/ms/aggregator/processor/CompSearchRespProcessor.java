
package se.sics.ms.aggregator.processor;

import se.sics.ktoolbox.aggregator.local.api.ComponentInfoProcessor;
import se.sics.ms.aggregator.SearchComponentInfo;
import se.sics.ms.aggregator.packets.SearchRespPacketInfo;

/**
 * A simple processor converting the information from the component
 * to the SearchResponse Packet Information.
 * Created by babbar on 2015-09-06.
 */
public class CompSearchRespProcessor implements ComponentInfoProcessor<SearchComponentInfo, SearchRespPacketInfo>{

    @Override
    public SearchRespPacketInfo processComponentInfo(SearchComponentInfo searchComponentInfo) {
        return new SearchRespPacketInfo(searchComponentInfo.getSearchResponse());
    }
}

