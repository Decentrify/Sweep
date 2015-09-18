package se.sics.ms.helper;

import se.sics.ktoolbox.aggregator.util.PacketInfo;
import se.sics.ktoolbox.aggregator.util.Processor;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Processor which enables to determine whether the object
 * has achieved the final state in the system.
 *
 *
 * This is a special kind of processor which runs on window by window basis.
 * Created by babbar on 2015-09-18.
 */
public interface FinalStateProcessor<PI extends PacketInfo, PI_O extends FinalStateInfo> extends Processor {


    /**
     * Processor needs to look at a particular window and then determine the
     * final states of the nodes in the system.
     *
     * @param window window for data collection.
     *
     * @return Collection of final states.
     */
    public Collection<PI_O> process(Map<Integer, List<PacketInfo>> window);
}
