package se.sics.p2ptoolbox.election.core.data;

import se.sics.gvod.net.VodAddress;
import se.sics.p2ptoolbox.election.api.LCPeerView;

import java.security.PublicKey;
import java.util.UUID;

/**
 * Container for the extension request sent by the leader in case it thinks
 * after the lease gets over he is still the leader.
 *
 * Created by babbarshaer on 2015-04-02.
 */
public class ExtensionRequest {

    public VodAddress leaderAddress;
    public PublicKey leaderPublicKey;
    public LCPeerView leaderView;
    public UUID electionRoundId; 
    
    public ExtensionRequest(VodAddress leaderAddress, PublicKey publicKey, LCPeerView leaderView, UUID electionRoundId){
        
        this.leaderAddress = leaderAddress;
        this.leaderPublicKey = publicKey;
        this.leaderView = leaderView;
        this.electionRoundId = electionRoundId;
    }
    
}
