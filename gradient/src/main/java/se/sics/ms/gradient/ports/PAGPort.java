package se.sics.ms.gradient.ports;

import se.sics.kompics.PortType;
import se.sics.ms.gradient.events.PAGUpdate;

/**
 * Main port for the interaction among the PAG 
 * and the application.
 *
 * Created by babbarshaer on 2015-06-03.
 */
public class PAGPort extends PortType{{
    request(PAGUpdate.class);
}}
