package se.sics.ms.gradient;

import se.sics.gvod.timer.TimeoutId;
import se.sics.kompics.Event;
import se.sics.kompics.PortType;
import se.sics.peersearch.messages.AddIndexEntryMessage;
import se.sics.peersearch.types.IndexEntry;
import se.sics.peersearch.types.SearchPattern;

public class GradientRoutingPort extends PortType {
	{
		negative(AddIndexEntryRequest.class);
        negative(IndexExchangeRequest.class);
        negative(ReplicationRequest.class);
        negative(SearchRequest.class);
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

    public static class IndexExchangeRequest extends Event {
        private final long lowestMissingIndexEntry;
        private final Long[] existingEntries;

        public IndexExchangeRequest(long lowestMissingIndexEntry, Long[] existingEntries) {
            this.lowestMissingIndexEntry = lowestMissingIndexEntry;
            this.existingEntries = existingEntries;
        }

        public long getLowestMissingIndexEntry() {
            return lowestMissingIndexEntry;
        }

        public Long[] getExistingEntries() {
            return existingEntries;
        }
    }

    public static class ReplicationRequest extends Event {
        private final IndexEntry entry;
        private final TimeoutId timeoutId;

        public ReplicationRequest(IndexEntry entry, TimeoutId timeoutId) {
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

    public static class SearchRequest extends Event {
        private final SearchPattern pattern;
        private final TimeoutId timeoutId;

        public SearchRequest(SearchPattern pattern, TimeoutId timeoutId) {
            this.pattern = pattern;
            this.timeoutId = timeoutId;
        }

        public SearchPattern getPattern() {
            return pattern;
        }

        public TimeoutId getTimeoutId() {
            return timeoutId;
        }
    }
}
