package se.sics.p2ptoolbox.election.core.data;

import se.sics.p2ptoolbox.croupier.api.util.PeerView;

/**
 * Container of the information passed during the voting request initiated
 * by the node that sees itself as the best for it to become the leader.
 *  
 * Created by babbarshaer on 2015-03-28.
 */
public class VotingRequest {
    
    public final PeerView leaderSelfView;
    
    public VotingRequest(PeerView selfView){
        this.leaderSelfView = selfView;
    }

}
