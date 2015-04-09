package se.sics.p2ptoolbox.election.core.data;

import se.sics.gvod.net.VodAddress;
import se.sics.p2ptoolbox.croupier.api.util.PeerView;
import se.sics.p2ptoolbox.election.api.LCPeerView;

import java.util.UUID;

/**
 * Promise Message Object which is 
 * sent between the nodes in the system as part 
 * of the Leader Election Protocol.
 *
 * Created by babbarshaer on 2015-03-29.
 */
public class Promise {

    
    public static class Request{
        
        public final LCPeerView leaderView;
        public final VodAddress leaderAddress;
        public final UUID electionRoundId;
        
        public Request(VodAddress leaderAddress, LCPeerView leaderView, UUID electionRoundId) {
            this.leaderAddress = leaderAddress;
            this.leaderView = leaderView;
            this.electionRoundId = electionRoundId;
        }
    }
    
    public static class Response{
        
        public final boolean acceptCandidate;
        public final boolean isConverged;
        public final UUID electionRoundId;
        
        public Response(boolean acceptCandidate, boolean isConverged, UUID electionRoundId){
            
            this.acceptCandidate = acceptCandidate;
            this.isConverged = isConverged;
            this.electionRoundId = electionRoundId;
        }
        
    }
}
