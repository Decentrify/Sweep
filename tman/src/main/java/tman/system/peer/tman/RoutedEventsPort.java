package tman.system.peer.tman;

import se.sics.kompics.PortType;
import se.sics.peersearch.messages.AddIndexEntryMessage;
import tman.system.peer.tman.LeaderRequest.AddIndexEntry;
import tman.system.peer.tman.LeaderRequest.GapCheck;

public class RoutedEventsPort extends PortType {
	{
		positive(AddIndexEntry.class);
		negative(AddIndexEntry.class);
		negative(AddIndexEntryMessage.Request.class);
		positive(GapCheck.class);
		negative(GapCheck.class);
        positive(AddIndexEntryMessage.Request.class);
	}
}
