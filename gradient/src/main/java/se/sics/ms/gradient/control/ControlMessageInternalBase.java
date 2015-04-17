package se.sics.ms.gradient.control;

import se.sics.gvod.net.VodAddress;
import se.sics.gvod.timer.TimeoutId;
import se.sics.kompics.Event;
import se.sics.p2ptoolbox.util.network.impl.DecoratedAddress;

/**
 * Created by alidar on 8/12/14.
 */
public class ControlMessageInternalBase extends Event {

    private DecoratedAddress sourceAddress;
    private TimeoutId roundId;

    public ControlMessageInternalBase(TimeoutId roundId, DecoratedAddress sourceAddress){
        this.sourceAddress = sourceAddress;
        this.roundId = roundId;
    }

    public DecoratedAddress getSourceAddress(){
        return this.sourceAddress;
    }

    public TimeoutId getRoundId(){
        return this.roundId;
    }

}
