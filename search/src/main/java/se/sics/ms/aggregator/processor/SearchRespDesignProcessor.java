package se.sics.ms.aggregator.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.ktoolbox.aggregator.server.event.AggregatedInfo;
import se.sics.ktoolbox.aggregator.server.util.DesignInfoContainer;
import se.sics.ktoolbox.aggregator.server.util.DesignProcessor;
import se.sics.ktoolbox.aggregator.util.PacketInfo;
import se.sics.ms.aggregator.design.AvgSearchRespContainer;
import se.sics.ms.aggregator.design.AvgSearchResponse;
import se.sics.ms.aggregator.SearchRespPacketInfo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 *
 * Created by babbar on 2015-09-06.
 */
public class SearchRespDesignProcessor implements DesignProcessor<SearchRespPacketInfo, AvgSearchResponse> {

    private Logger logger = LoggerFactory.getLogger(SearchRespDesignProcessor.class);

    @Override
    public DesignInfoContainer<AvgSearchResponse> process(List<AggregatedInfo> collection) {

        logger.debug("Initiating the processing of the system information map.");

        Collection<AvgSearchResponse> collectionResult = new ArrayList<AvgSearchResponse>();
        for(AggregatedInfo window : collection){

            int count = 0;
            float sum = 0;

            Map<Integer, List<PacketInfo>> map = window.getNodePacketMap();
            for(Map.Entry<Integer, List<PacketInfo>> entry: map.entrySet()){

                for(PacketInfo info : entry.getValue()){
                    if( info instanceof  SearchRespPacketInfo){

                        count ++;
                        SearchRespPacketInfo srpi = (SearchRespPacketInfo) info;
                        sum += srpi.getSearchResponse();
                    }
                }
            }

            if(count > 0){
                float average = sum / count;
                AvgSearchResponse psdi = new AvgSearchResponse(average, count);
                collectionResult.add(psdi);
            }
        }

        return new AvgSearchRespContainer(collectionResult);
    }



    @Override
    public void cleanState() {
        logger.debug("Call to clear any internal state.");
    }
}
