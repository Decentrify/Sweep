package se.sics.ms.data.aggregator.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.ktoolbox.aggregator.server.util.DesignInfoContainer;
import se.sics.ktoolbox.aggregator.server.util.DesignProcessor;
import se.sics.ktoolbox.aggregator.util.PacketInfo;
import se.sics.ms.data.aggregator.design.AvgSearchRespContainer;
import se.sics.ms.data.aggregator.design.AvgSearchResponse;
import se.sics.ms.data.aggregator.packets.SearchRespPacketInfo;
import se.sics.p2ptoolbox.util.network.impl.BasicAddress;

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
    public DesignInfoContainer<AvgSearchResponse> process(Collection<Map<Integer, List<PacketInfo>>> collection) {

        logger.debug("Initiating the processing of the system information map.");

        Collection<AvgSearchResponse> collectionResult = new ArrayList<AvgSearchResponse>();
        for(Map<Integer, List<PacketInfo>> window : collection){

            int count = 0;
            float sum = 0;

            for(Map.Entry<Integer, List<PacketInfo>> entry: window.entrySet()){

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
