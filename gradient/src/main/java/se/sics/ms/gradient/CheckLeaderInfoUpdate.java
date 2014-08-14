package se.sics.ms.gradient;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.timer.TimeoutId;

import java.util.LinkedList;

/**
 * Created by alidar on 8/12/14.
 */

public class CheckLeaderInfoUpdate {

    public static class Request extends ControlMessageInternal.Request {
        public Request(TimeoutId roundId , VodAddress sourceAddress){
            super(roundId, sourceAddress);
        }
    }

    public static class Response extends ControlMessageInternal.Response {

        private VodAddress leader;

        public Response(TimeoutId roundId, VodAddress sourceAddress, VodAddress leader){
            super(roundId, sourceAddress, ControlMessageEnum.LEADER_UPDATE);
            this.leader = leader;
        }

        public VodAddress getLeader() {
            return leader;
        }
    }
}
