package se.sics.p2ptoolbox.election.api.msg;

import se.sics.gvod.address.Address;
import se.sics.gvod.net.VodAddress;
import se.sics.kompics.KompicsEvent;

import java.security.PublicKey;
import java.util.Collection;

/**
 * Indication event from the election component,
 * stating the information about the current leader.
 * 
 * <br/>
 *
 * It also provides information regarding the leader group membership. 
 * By looking at the information a node can decide whether he is in the group and therefore based on the lease time
 * can update itself.
 *
 * Created by babbarshaer on 2015-03-27.
 */
public class LeaderUpdate implements KompicsEvent{

    public final PublicKey leaderPublicKey;
    public final VodAddress leaderAddress;
    
    public LeaderUpdate(PublicKey leaderPublicKey, VodAddress leaderAddress){
        this.leaderPublicKey = leaderPublicKey;
        this.leaderAddress = leaderAddress;
    }

}
