package se.sics.p2ptoolbox.election.core.data;

import se.sics.gvod.common.msgs.DirectMsgNetty;
import se.sics.gvod.net.VodAddress;
import se.sics.p2ptoolbox.croupier.api.util.PeerView;

import java.security.PublicKey;

/**
 * Container class for the information exchanged between the node trying to 
 * assert itself as leader and the nodes in the system that it thinks should be 
 * in the leader group.
 *
 * Created by babbarshaer on 2015-03-29.
 */
public class LeaseCommit {
    
    
    public static class Request{
        
        public VodAddress leaderAddress;
        public PublicKey leaderPublicKey;
        public PeerView selfView;
        
        public Request(VodAddress leaderAddress, PublicKey publicKey, PeerView selfView){
            this.leaderAddress = leaderAddress;
            this.leaderPublicKey = publicKey;
            this.selfView = selfView;
        }
    }
    
    
    public static class Response{

        public final boolean isAccepted;
        
        public Response(boolean isAccepted){
            this.isAccepted = isAccepted;
        }
        
    }
    
}
