package se.sics.ms.gradient;

import se.sics.kompics.PortType;
import se.sics.peersearch.messages.AddIndexEntryMessage;
import se.sics.ms.gradient.LeaderRequest.AddIndexEntry;
import se.sics.ms.gradient.LeaderRequest.GapCheck;

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
