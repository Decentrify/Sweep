package se.kth.ms.api.port;

import se.sics.kompics.PortType;
import se.sics.ms.gradient.events.LUCheck;
import se.sics.ms.gradient.events.NPEvent;
import se.sics.ms.gradient.events.PAGUpdate;

/**
 * Main port for the interaction among the PAG 
 * and the application.
 *
 * Created by babbarshaer on 2015-06-03.
 */
public class PAGPort extends PortType{{
    
    request(PAGUpdate.class);
    indication(NPEvent.class);

    indication(LUCheck.Request.class);
    request(LUCheck.Response.class);
}}
