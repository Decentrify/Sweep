package se.sics.ms.gradient.events;

import se.sics.gvod.net.VodAddress;
import se.sics.kompics.Event;

import java.security.PublicKey;

/**
 * Created by alidar on 8/13/14.
 */
public class LeaderInfoUpdate extends Event {

    private VodAddress leaderAddress;
    private PublicKey leaderPublicKey;

    public LeaderInfoUpdate(VodAddress leaderAddress, PublicKey leaderPublicKey) {
        this.leaderAddress = leaderAddress;
        this.leaderPublicKey = leaderPublicKey;
    }


    public VodAddress getLeaderAddress() {
        return leaderAddress;
    }

    public PublicKey getLeaderPublicKey() {
        return leaderPublicKey;
    }
}
