package se.sics.p2ptoolbox.election.example.data;

import se.sics.gvod.net.VodAddress;
import se.sics.kompics.KompicsEvent;

import java.util.Collection;

/**
 * Event sent by the application to the mocked component informing it about the peers in the system.
 * Created by babbar on 2015-04-01.
 */
public class PeersUpdate implements KompicsEvent{

    public Collection<VodAddress> peers;

    public PeersUpdate(Collection<VodAddress> peers){
        this.peers = peers;
    }
}
