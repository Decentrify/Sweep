package se.sics.p2ptoolbox.election.core.data;

import se.sics.gvod.net.VodAddress;
import se.sics.p2ptoolbox.election.api.LCPeerView;

import java.security.PublicKey;
import java.util.UUID;

/**
 * Container class for the information exchanged between the node trying to 
 * assert itself as leader and the nodes in the system that it thinks should be 
 * in the leader group.
 *
 * Created by babbarshaer on 2015-03-29.
 */
public class LeaseCommitUpdated {

    
    public static class Request {
        
        public VodAddress leaderAddress;
        public PublicKey leaderPublicKey;
        public LCPeerView leaderView;
        public UUID electionRoundId;
        
        public Request(VodAddress leaderAddress, PublicKey publicKey, LCPeerView leaderView, UUID electionRoundId){
            this.leaderAddress = leaderAddress;
            this.leaderPublicKey = publicKey;
            this.leaderView = leaderView;
            this.electionRoundId = electionRoundId;
        }
            
    }

    public static class Response {
        
        public boolean isCommit;
        public UUID electionRoundId;
        
        public Response(boolean isCommit, UUID electionRoundId){
            this.isCommit = isCommit;
            this.electionRoundId = electionRoundId;
        }
    }

}
