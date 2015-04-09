package se.sics.p2ptoolbox.election.api.msg;

import se.sics.kompics.KompicsEvent;

import java.util.UUID;

/**
 * Wrapper for the marker events to let the application know about the node being elected / removed
 * to / from the leader group.
 *
 * Created by babbar on 2015-03-31.
 */
public class ElectionState{

    public static class EnableLGMembership implements KompicsEvent{
        
        public UUID  electionRoundId;
        public EnableLGMembership(UUID electionRoundId){
            this.electionRoundId = electionRoundId;
        }
    }

    public static class DisableLGMembership implements KompicsEvent{
        
        public UUID  electionRoundId;
        public DisableLGMembership(UUID electionRoundId){
            this.electionRoundId = electionRoundId;
        }
    }

}
