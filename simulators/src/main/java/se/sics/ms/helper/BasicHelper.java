package se.sics.ms.helper;

import se.sics.ktoolbox.aggregator.server.event.AggregatedInfo;
import se.sics.ktoolbox.aggregator.util.PacketInfo;
import se.sics.ms.data.InternalStatePacket;
import se.sics.ms.main.AggregatorCompHelper;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Basic Helper for the aggregator host component
 * filtering the aggregated information based on type of packet.
 *
 * Created by babbarshaer on 2015-09-20.
 */
public class BasicHelper implements AggregatorCompHelper {


    /**
     * As per the requirement, filtering everything except for the
     * Internal State Packets in the node packetMap.
     *
     * @param originalInfo original information.
     * @return FilteredInformation.
     */
    public AggregatedInfo filter(AggregatedInfo originalInfo) {


        Map<Integer, List<PacketInfo>> nodePacketMap = new HashMap<Integer, List<PacketInfo>>(originalInfo.getNodePacketMap());

        for(Map.Entry<Integer, List<PacketInfo>> entry : nodePacketMap.entrySet()){

            Iterator<PacketInfo>  packetInfoIterator = entry.getValue().iterator();
            while(packetInfoIterator.hasNext()){

                PacketInfo packetInfo = packetInfoIterator.next();
                if(! (packetInfo instanceof InternalStatePacket)){

//                  Clean everything except the InternalStatePacket.
                    packetInfoIterator.remove();
                }
            }
        }

        return originalInfo;
    }
}
