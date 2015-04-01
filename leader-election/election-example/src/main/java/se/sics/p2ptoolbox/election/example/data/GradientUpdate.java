package se.sics.p2ptoolbox.election.example.data;

import se.sics.kompics.KompicsEvent;
import se.sics.p2ptoolbox.croupier.api.util.CroupierPeerView;

import java.util.Collection;

/**
 * Event sent by the mocked component to the Leader Election component.
 * Created by babbar on 2015-04-01.
 */
public class GradientUpdate implements KompicsEvent {

    Collection<CroupierPeerView> cpvCollection;

    public GradientUpdate(Collection<CroupierPeerView> cpvCollection){
        this.cpvCollection = cpvCollection;
    }

}
