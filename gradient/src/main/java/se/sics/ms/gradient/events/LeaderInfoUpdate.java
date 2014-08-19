package se.sics.ms.gradient.events;

import se.sics.gvod.net.VodAddress;
import se.sics.kompics.Event;
/**
 * Created by alidar on 8/13/14.
 */
public class LeaderInfoUpdate extends Event {

    private VodAddress leaderAddress;

    public LeaderInfoUpdate(VodAddress leaderAddress) {
        this.setLeaderAddress(leaderAddress);
    }


    public VodAddress getLeaderAddress() {
        return leaderAddress;
    }

    public void setLeaderAddress(VodAddress leaderAddress) {
        this.leaderAddress = leaderAddress;
    }
}
