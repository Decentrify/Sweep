package se.sics.p2ptoolbox.election.api.ports;

import se.sics.kompics.PortType;
import se.sics.p2ptoolbox.election.api.msg.ElectionState;
import se.sics.p2ptoolbox.election.api.msg.LeaderState;
import se.sics.p2ptoolbox.election.api.msg.LeaderUpdate;
import se.sics.p2ptoolbox.election.api.msg.ViewUpdate;

/**
 * Main Port to communicate with the Leader Election Module.
 * Created by babbarshaer on 2015-03-27.
 */
public class LeaderElectionPort extends PortType{{
    
    request(ViewUpdate.class);
    indication(LeaderUpdate.class);

    indication(LeaderState.ElectedAsLeader.class);
    indication(LeaderState.TerminateBeingLeader.class);

    indication(ElectionState.EnableLGMembership.class);
    indication(ElectionState.DisableLGMembership.class);

}}