package se.sics.ms.gradient.control;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.timer.TimeoutId;

import java.security.PublicKey;

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
        private PublicKey leaderPublicKey;

        public Response(TimeoutId roundId, VodAddress sourceAddress, VodAddress leader, PublicKey leaderPublicKey){
            super(roundId, sourceAddress, ControlMessageEnum.LEADER_UPDATE);
            this.leader = leader;
            this.leaderPublicKey = leaderPublicKey;
        }

        public VodAddress getLeader() {
            return leader;
        }

        public PublicKey getLeaderPublicKey() {
            return leaderPublicKey;
        }
    }
}
