package se.sics.ms.gradient.events;

import se.sics.kompics.KompicsEvent;

import java.security.PublicKey;
import se.sics.ktoolbox.util.network.KAddress;

/**
 * Update about the current leader in the partition.
 *
 * Created by alidar on 8/13/14.
 */
public class LeaderInfoUpdate implements KompicsEvent {

    private KAddress leaderAddress;
    private PublicKey leaderPublicKey;

    public LeaderInfoUpdate(KAddress leaderAddress, PublicKey leaderPublicKey) {
        this.leaderAddress = leaderAddress;
        this.leaderPublicKey = leaderPublicKey;
    }


    public KAddress getLeaderAddress() {
        return leaderAddress;
    }

    public PublicKey getLeaderPublicKey() {
        return leaderPublicKey;
    }
}
