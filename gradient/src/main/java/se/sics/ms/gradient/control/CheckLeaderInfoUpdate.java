package se.sics.ms.gradient.control;
import se.sics.p2ptoolbox.util.network.impl.DecoratedAddress;

import java.security.PublicKey;
import java.util.UUID;

/**
 * Internal message requesting the current leader information,
 * from the component.
 *
 * Created by alidar on 8/12/14.
 */

public class CheckLeaderInfoUpdate {

    public static class Request extends ControlMessageInternal.Request {
        
        public Request(UUID roundId , DecoratedAddress sourceAddress){
            super(roundId, sourceAddress);
        }
    }

    public static class Response extends ControlMessageInternal.Response {

        private DecoratedAddress leader;
        private PublicKey leaderPublicKey;

        public Response(UUID roundId, DecoratedAddress sourceAddress, DecoratedAddress leader, PublicKey leaderPublicKey){
            super(roundId, sourceAddress, ControlMessageEnum.LEADER_UPDATE);
            this.leader = leader;
            this.leaderPublicKey = leaderPublicKey;
        }

        public DecoratedAddress getLeader() {
            return leader;
        }

        public PublicKey getLeaderPublicKey() {
            return leaderPublicKey;
        }
    }
}
