package se.sics.ms.peer;

import se.sics.kompics.PortType;

public class PeerPort extends PortType {
	{
		negative(JoinPeer.class);
	}
}