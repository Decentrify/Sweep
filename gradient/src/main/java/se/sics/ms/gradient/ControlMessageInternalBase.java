package se.sics.ms.gradient;

import se.sics.gvod.net.VodAddress;
import se.sics.gvod.timer.TimeoutId;
import se.sics.kompics.Event;

/**
 * Created by alidar on 8/12/14.
 */
public class ControlMessageInternalBase extends Event {

    private VodAddress sourceAddress;
    private TimeoutId roundId;

    public ControlMessageInternalBase(TimeoutId roundId, VodAddress sourceAddress){
        this.sourceAddress = sourceAddress;
        this.roundId = roundId;
    }

    public VodAddress getSourceAddress(){
        return this.sourceAddress;
    }

    public TimeoutId getRoundId(){
        return this.roundId;
    }

}
