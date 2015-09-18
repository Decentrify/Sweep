package se.sics.ms.helper;

import se.sics.ktoolbox.aggregator.util.PacketInfo;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Final Processor for the entry state processor.
 *
 * Created by babbar on 2015-09-18.
 */
public class EntryFinalStateProcessor implements FinalStateProcessor<PacketInfo, EntryFinalState>{

    @Override
    public Collection<EntryFinalState> process(Map<Integer, List<PacketInfo>> window) {
        return null;
    }
}
