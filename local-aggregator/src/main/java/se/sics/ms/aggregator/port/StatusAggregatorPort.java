package se.sics.ms.aggregator.port;

import se.sics.kompics.PortType;
import se.sics.ms.aggregator.type.ComponentUpdateEvent;

/**
 * Status Aggregator Component Port.
 *
 * Created by babbarshaer on 2015-02-19.
 */
public class StatusAggregatorPort extends PortType{{
    
    request(ComponentUpdateEvent.class);
}}
