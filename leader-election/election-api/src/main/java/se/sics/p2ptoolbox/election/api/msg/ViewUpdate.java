package se.sics.p2ptoolbox.election.api.msg;

import se.sics.kompics.KompicsEvent;
import se.sics.p2ptoolbox.croupier.api.util.PeerView;
import se.sics.p2ptoolbox.election.api.LCPeerView;

import java.util.UUID;

/**
 * Event from application in the system, indicating
 * updated value of self view.
 *
 * Created by babbarshaer on 2015-03-27.
 */
public class ViewUpdate implements KompicsEvent{
    
    public final LCPeerView selfPv;
    public final UUID electionRoundId;
    
    public ViewUpdate(UUID electionRoundId, LCPeerView pv){
        this.selfPv = pv;
        this.electionRoundId = electionRoundId;
    }
}
