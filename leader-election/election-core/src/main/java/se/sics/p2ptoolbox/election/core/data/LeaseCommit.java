package se.sics.p2ptoolbox.election.core.data;

import se.sics.gvod.common.msgs.DirectMsgNetty;
import se.sics.gvod.net.VodAddress;
import se.sics.p2ptoolbox.croupier.api.util.PeerView;
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
public class LeaseCommit {
        
        public VodAddress leaderAddress;
        public PublicKey leaderPublicKey;
        public LCPeerView leaderView;

        public LeaseCommit(VodAddress leaderAddress, PublicKey publicKey, LCPeerView leaderView){
            this.leaderAddress = leaderAddress;
            this.leaderPublicKey = publicKey;
            this.leaderView = leaderView;
        }
}
