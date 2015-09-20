package se.sics.ms.helper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
    public DesignInfoContainer<ReplicationLagDesignInfo> process(Collection<Map<Integer, List<PacketInfo>>> collection) {

        logger.debug("Initiating the processing of the design information.");

        List<ReplicationLagDesignInfo> designInfoList = new ArrayList<ReplicationLagDesignInfo>();

        Iterator<Map<Integer, List<PacketInfo>>> iterator = collection.iterator();
        while(iterator.hasNext()){

            Map<Integer, List<PacketInfo>> window = iterator.next();
            List<Long> entryList = new ArrayList<Long>();

            for(Map.Entry<Integer, List<PacketInfo>> entry : window.entrySet()){

                for(PacketInfo packet : entry.getValue()){

                    if(packet instanceof InternalStatePacket){
                        entryList.add(((InternalStatePacket) packet).getNumEntries());
                    }
                }
            }



        }

//      Return the processed windows.
        return new ReplicationLagDesignInfoContainer(designInfoList);
    }




    /**
     * Process the list of the values for the replication lags.
     */
    private ReplicationLagDesignInfo calculateReplicationLag(List<Long> entries){

        logger.debug(" Create Replication Lag Design Information for all the ");

        if(entries.size() <= 0){
            return null;
        }


        Collections.sort(entries);
        long maxVal = entries.get(entries.size()-1);
        long minVal = entries.get(0);

        Collection<Long> difference = new ArrayList<Long>();

        return null;
    }


    @Override
    public void cleanState() {

    }
}
