package se.kth.ms.partitionaware.api.port;

import se.kth.ms.partitionaware.api.events.LUCheck;
import se.kth.ms.partitionaware.api.events.NPEvent;
import se.kth.ms.partitionaware.api.events.PALUpdate;
import se.sics.kompics.PortType;

/**
 * Main port for the interaction among the PAG 
 * and the application.
 *
 * Created by babbarshaer on 2015-06-03.
 */
public class PALPort extends PortType{{
    
    request(PALUpdate.class);
    indication(NPEvent.class);

    indication(LUCheck.Request.class);
    request(LUCheck.Response.class);
}}
