package se.sics.p2ptoolbox.election.core.util;

import se.sics.p2ptoolbox.election.api.LCPeerView;

/**
 * Interface used by the leader to filter the samples,
 * at specific points in time.
 *
 * Created by babbarshaer on 2015-03-27.
 */
public interface LeaderFilter {

    /**
     * Based on the application protocol, let them decide when do I need to terminate being the leader in case of change
     * of the view ?
     *
     * @param old old view
     * @param updated current view
     * @return terminate leadership
     */
    public boolean terminateLeader(LCPeerView old, LCPeerView updated);

}
