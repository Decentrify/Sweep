package se.sics.util;

import se.sics.p2ptoolbox.election.api.LCPeerView;
import se.sics.p2ptoolbox.election.core.util.LeaderFilter;

/**
 * Filter used by the leader election protocol.
 *
 * Created by babbar on 2015-04-06.
 */
public class SweepLeaderFilter implements LeaderFilter{

    @Override
    public boolean terminateLeader(LCPeerView old, LCPeerView updated) {
        return false;
    }
}
