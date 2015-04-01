package se.sics.p2ptoolbox.election.api.ports;

import se.sics.kompics.PortType;
import se.sics.p2ptoolbox.election.api.msg.mock.MockedGradientUpdate;

/**
 * Port for testing purposes on which the leader election algorithm handles the request from
 * the mock up components.
 * Created by babbar on 2015-04-01.
 */
public class TestPort extends PortType{{
    request(MockedGradientUpdate.class);
}}
