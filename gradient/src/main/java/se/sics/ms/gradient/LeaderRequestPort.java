package se.sics.ms.gradient;

import se.sics.gvod.timer.TimeoutId;
import se.sics.kompics.Event;
import se.sics.kompics.PortType;
import se.sics.peersearch.messages.AddIndexEntryMessage;
import se.sics.peersearch.types.IndexEntry;

public class LeaderRequestPort extends PortType {
	{
		negative(AddIndexEntryRequest.class);
	}

    public static class AddIndexEntryRequest extends Event {
        private final IndexEntry entry;
        private final TimeoutId timeoutId;

        public AddIndexEntryRequest(IndexEntry entry, TimeoutId timeoutId) {

            this.entry = entry;
            this.timeoutId = timeoutId;
        }

        public IndexEntry getEntry() {
            return entry;
        }

        public TimeoutId getTimeoutId() {
            return timeoutId;
        }
    }
}
