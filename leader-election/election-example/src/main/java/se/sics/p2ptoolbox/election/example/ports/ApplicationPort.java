package se.sics.p2ptoolbox.election.example.ports;

import se.sics.kompics.PortType;
import se.sics.p2ptoolbox.election.example.data.PeersUpdate;

/**
 *
 * Created by babbar on 2015-04-01.
 */
public class ApplicationPort extends PortType{{
    indication(PeersUpdate.class);
}}
