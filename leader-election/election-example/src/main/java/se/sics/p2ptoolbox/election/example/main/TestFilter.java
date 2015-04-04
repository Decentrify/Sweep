package se.sics.p2ptoolbox.election.example.main;

import se.sics.p2ptoolbox.election.api.LCPeerView;
import se.sics.p2ptoolbox.election.core.util.LeaderFilter;

/**
 * Test Filter for the application.
 *
 * Created by babbar on 2015-04-04.
 */
public class TestFilter implements LeaderFilter{

    @Override
    public boolean terminateLeader(LCPeerView old, LCPeerView updated) {
        return false;
    }
}
