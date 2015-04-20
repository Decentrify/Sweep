package se.sics.ms.gradient.control;

import se.sics.gvod.timer.TimeoutId;
import se.sics.p2ptoolbox.util.network.impl.DecoratedAddress;

import java.util.UUID;

/**
 * Created by alidar on 8/13/14.
 */
public class ControlMessageInternal {

    public static class Request extends ControlMessageInternalBase {

        public Request(UUID roundId, DecoratedAddress sourceAddress){
            super(roundId, sourceAddress);
        }

    }

    public static class Response extends ControlMessageInternalBase {

        private ControlMessageEnum controlMessageEnum;

        public Response(UUID roundId, DecoratedAddress sourceAddress, ControlMessageEnum controlMessageEnum) {
            super(roundId, sourceAddress);
            this.controlMessageEnum = controlMessageEnum;
        }

        public ControlMessageEnum getControlMessageEnum(){
            return this.controlMessageEnum;
        }
    }
}
