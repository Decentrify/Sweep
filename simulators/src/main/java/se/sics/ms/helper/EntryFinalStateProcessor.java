package se.sics.ms.helper;

import se.sics.ktoolbox.aggregator.util.PacketInfo;
import se.sics.ms.data.InternalStatePacket;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Final Processor for the entry state processor.
 *
 * Created by babbar on 2015-09-18.
 */
public class EntryFinalStateProcessor implements FinalStateProcessor<PacketInfo, EntryFinalState>{



    public Collection<EntryFinalState> process(Map<Integer, List<PacketInfo>> window) {

        Collection<EntryFinalState> finalStates = new ArrayList<EntryFinalState>();

        for(Map.Entry<Integer, List<PacketInfo>> entry : window.entrySet()){
            for(PacketInfo packetInfo : entry.getValue()){

                if(packetInfo instanceof InternalStatePacket){
                    InternalStatePacket isp = (InternalStatePacket)packetInfo;
                    EntryFinalState efs = new EntryFinalState(isp.getNumEntries());
                    finalStates.add(efs);
                }
            }
        }

        return finalStates;
    }
}
