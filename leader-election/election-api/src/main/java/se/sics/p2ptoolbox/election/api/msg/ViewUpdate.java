package se.sics.p2ptoolbox.election.api.msg;

import se.sics.kompics.KompicsEvent;
import se.sics.p2ptoolbox.croupier.api.util.PeerView;

/**
 * Event from application in the system, indicating
 * updated value of self view.
 *
 * Created by babbarshaer on 2015-03-27.
 */
public class ViewUpdate implements KompicsEvent{
    
    public final PeerView selfPv;
    
    public ViewUpdate(PeerView pv){
        this.selfPv = pv;
    }
}
