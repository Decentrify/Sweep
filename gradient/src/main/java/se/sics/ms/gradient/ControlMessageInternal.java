package se.sics.ms.gradient;

import se.sics.gvod.net.VodAddress;
import se.sics.gvod.timer.TimeoutId;
/**
 * Created by alidar on 8/13/14.
 */
public class ControlMessageInternal {

    public static class Request extends ControlMessageInternalBase {

        public Request(TimeoutId roundId, VodAddress sourceAddress){
            super(roundId, sourceAddress);
        }

    }

    public static class Response extends ControlMessageInternalBase {

        private ControlMessageEnum controlMessageEnum;

        public Response(TimeoutId roundId, VodAddress sourceAddress, ControlMessageEnum controlMessageEnum) {
            super(roundId, sourceAddress);
            this.controlMessageEnum = controlMessageEnum;
        }

        public ControlMessageEnum getControlMessageEnum(){
            return this.controlMessageEnum;
        }
    }
}
