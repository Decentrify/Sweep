package se.sics.ms.ports;

import se.sics.kompics.PortType;
import se.sics.ms.types.ComponentUpdateEvent;

/**
 * Status Aggregator Component Port.
 *
 * Created by babbarshaer on 2015-02-19.
 */
public class StatusAggregatorPort extends PortType{{
    
    request(ComponentUpdateEvent.class);
}}
