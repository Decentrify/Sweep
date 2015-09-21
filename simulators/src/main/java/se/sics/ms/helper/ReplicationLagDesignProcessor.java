package se.sics.ms.helper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.ktoolbox.aggregator.server.event.AggregatedInfo;
import se.sics.ktoolbox.aggregator.server.util.DesignInfoContainer;
import se.sics.ktoolbox.aggregator.server.util.DesignProcessor;
import se.sics.ktoolbox.aggregator.util.PacketInfo;
import se.sics.ms.data.InternalStatePacket;

import java.util.*;

/**
 * Processor for the Replication Lag Design Information.
 *
 * Created by babbar on 2015-09-20.
 */
public class ReplicationLagDesignProcessor implements DesignProcessor<PacketInfo, ReplicationLagDesignInfo> {


    private Logger logger = LoggerFactory.getLogger(ReplicationLagDesignProcessor.class);


    @Override
    public DesignInfoContainer<ReplicationLagDesignInfo> process(List<AggregatedInfo> windows) {

        logger.debug("Initiating the processing of the design information.");
        List<ReplicationLagDesignInfo> designInfoList = new ArrayList<ReplicationLagDesignInfo>();

        Iterator<AggregatedInfo> iterator = windows.iterator();
        while(iterator.hasNext()){

            AggregatedInfo window = iterator.next();
            Map<Integer, List<PacketInfo>> map = window.getNodePacketMap();
            List<Long> entryList = new ArrayList<Long>();

            for(Map.Entry<Integer, List<PacketInfo>> entry : map.entrySet()){
                for(PacketInfo packet : entry.getValue()){

                    if(packet instanceof InternalStatePacket){
                        entryList.add(((InternalStatePacket) packet).getNumEntries());
                    }
                }
            }

            ReplicationLagDesignInfo designInfo = calculateReplicationLag(window.getTime(), entryList);
            if(designInfo == null)
                continue;

            designInfoList.add(designInfo);
        }

//      Return the processed windows.
        return new ReplicationLagDesignInfoContainer(designInfoList);
    }




    /**
     * Process the list of the values for the replication lags.
     */
    private ReplicationLagDesignInfo calculateReplicationLag(long time, List<Long> entries){

        if(entries.size() <= 0){
            return null;
        }

        Collections.sort(entries);
        long maxVal = entries.get(entries.size()-1);
        long minVal = entries.get(0);

        long sum = 0;

        for(Long val : entries){
            sum += (maxVal - val);
        }

        double avg = sum/(double)entries.size();
        return new ReplicationLagDesignInfo(time, avg, maxVal-minVal, 0);
    }


    @Override
    public void cleanState() {
        logger.debug("Call to clean the internal state of the processor.");
    }
}
