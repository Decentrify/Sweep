package se.sics.ms.gradient.events;

import se.sics.gvod.net.VodAddress;
import se.sics.kompics.Event;
import se.sics.kompics.KompicsEvent;
import se.sics.p2ptoolbox.util.network.impl.DecoratedAddress;

import java.security.PublicKey;

/**
 * Update about the current leader in the partition.
 *
 * Created by alidar on 8/13/14.
 */
public class LeaderInfoUpdate implements KompicsEvent {

    private DecoratedAddress leaderAddress;
    private PublicKey leaderPublicKey;

    public LeaderInfoUpdate(DecoratedAddress leaderAddress, PublicKey leaderPublicKey) {
        this.leaderAddress = leaderAddress;
        this.leaderPublicKey = leaderPublicKey;
    }


    public DecoratedAddress getLeaderAddress() {
        return leaderAddress;
    }

    public PublicKey getLeaderPublicKey() {
        return leaderPublicKey;
    }
}
