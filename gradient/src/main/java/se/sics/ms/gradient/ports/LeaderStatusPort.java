package se.sics.ms.gradient.ports;

import se.sics.kompics.PortType;
import se.sics.ms.gradient.events.LeaderInfoUpdate;

public class LeaderStatusPort extends PortType {{
        indication(LeaderInfoUpdate.class);
}}
