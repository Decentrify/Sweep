package se.sics.ms.gradient.control;

import se.sics.kompics.Event;
import se.sics.p2ptoolbox.util.network.impl.DecoratedAddress;

import java.util.UUID;

/**
 * Base class for the control message information flow within the components.
 * Created by alidar on 8/12/14.
 */
public class ControlMessageInternalBase extends Event {

    private DecoratedAddress sourceAddress;
    private UUID roundId;

    public ControlMessageInternalBase(UUID roundId, DecoratedAddress sourceAddress){
        this.sourceAddress = sourceAddress;
        this.roundId = roundId;
    }

    public DecoratedAddress getSourceAddress(){
        return this.sourceAddress;
    }

    public UUID getRoundId(){
        return this.roundId;
    }

}
