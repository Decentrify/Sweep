package se.sics.ms.gradient;

import se.sics.gvod.timer.TimeoutId;
import se.sics.kompics.Event;
import se.sics.kompics.PortType;
import se.sics.ms.messages.PartitionMessage;
import se.sics.ms.types.IndexEntry;
import se.sics.ms.types.SearchPattern;

public class GradientRoutingPort extends PortType {
	{
		negative(AddIndexEntryRequest.class);
        negative(IndexHashExchangeRequest.class);
        negative(SearchRequest.class);
        negative(ReplicationPrepareCommitRequest.class);
        negative(ReplicationCommit.class);
        negative(SearchRequest.class);
        negative(ViewSizeMessage.Request.class);
        positive(ViewSizeMessage.Response.class);
        negative(PartitionMessage.class);
        positive(RemoveEntriesNotFromYourPartition.class);
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

    public static class IndexHashExchangeRequest extends Event {
        private final long lowestMissingIndexEntry;
        private final Long[] existingEntries;
        private final TimeoutId timeoutId;
        private final int numberOfRequests;

        public IndexHashExchangeRequest(long lowestMissingIndexEntry, Long[] existingEntries, TimeoutId timeoutId, int numberOfRequests) {
            this.lowestMissingIndexEntry = lowestMissingIndexEntry;
            this.existingEntries = existingEntries;
            this.timeoutId = timeoutId;
            this.numberOfRequests = numberOfRequests;
        }

        public long getLowestMissingIndexEntry() {
            return lowestMissingIndexEntry;
        }

        public Long[] getExistingEntries() {
            return existingEntries;
        }

        public TimeoutId getTimeoutId() {
            return timeoutId;
        }

        public int getNumberOfRequests() {
            return numberOfRequests;
        }
    }

    public static class SearchRequest extends Event {
        private final SearchPattern pattern;
        private final TimeoutId timeoutId;
        private final int queryTimeout;

        public SearchRequest(SearchPattern pattern, TimeoutId timeoutId, int queryTimeout) {
            this.pattern = pattern;
            this.timeoutId = timeoutId;
            this.queryTimeout = queryTimeout;
        }

        public SearchPattern getPattern() {
            return pattern;
        }

        public TimeoutId getTimeoutId() {
            return timeoutId;
        }

        public int getQueryTimeout() {
            return queryTimeout;
        }
    }

    public static class ReplicationPrepareCommitRequest extends Event {
        private final IndexEntry entry;
        private final TimeoutId timeoutId;

        public ReplicationPrepareCommitRequest(IndexEntry entry, TimeoutId timeoutId) {
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

    public static class ReplicationCommit extends Event {
        private final TimeoutId timeoutId;
        private final Long indexEntryId;
        private final String signature;

        public ReplicationCommit(TimeoutId timeoutId, Long indexEntryId, String signature) {
            this.timeoutId = timeoutId;
            this.indexEntryId = indexEntryId;
            this.signature = signature;
        }

        public Long getIndexEntryId() {
            return indexEntryId;
        }

        public TimeoutId getTimeoutId() {
            return timeoutId;
        }

        public String getSignature() {
            return signature;
        }
    }
}
