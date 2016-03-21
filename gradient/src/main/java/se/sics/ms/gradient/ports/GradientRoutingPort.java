package se.sics.ms.gradient.ports;

import java.util.ArrayList;
import se.sics.kompics.Event;
import se.sics.kompics.KompicsEvent;
import se.sics.kompics.PortType;
import se.sics.ms.gradient.events.*;
import se.sics.ms.types.IndexEntry;
import se.sics.ms.types.SearchPattern;
import se.sics.ms.util.PartitionHelper;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import se.sics.ktoolbox.util.network.KAddress;

public class GradientRoutingPort extends PortType {

    {
        negative(AddIndexEntryRequest.class);
        negative(IndexHashExchangeRequest.class);
        positive(IndexHashExchangeResponse.class);
        negative(SearchRequest.class);
        negative(SearchRequest.class);
        positive(RemoveEntriesNotFromYourPartition.class);
        positive(NumberOfPartitions.class);

        // Two Phase Commit Partitioning Messages.
        negative(ApplyPartitioningUpdate.class);

        // Generic Pull Based Control Message Exchange.
        negative(InitiateControlMessageExchangeRound.class);

    }

    /**
     * Simply inform the gradient component to begin the control message
     * exchange.
     */
    public static class InitiateControlMessageExchangeRound extends Event {

        private UUID roundId;
        private int controlMessageExchangeNumber;

        public InitiateControlMessageExchangeRound(UUID roundId, int controlMessageExchangeNumber) {
            this.roundId = roundId;
            this.controlMessageExchangeNumber = controlMessageExchangeNumber;
        }

        public UUID getRoundId() {
            return this.roundId;
        }

        public int getControlMessageExchangeNumber() {
            return this.controlMessageExchangeNumber;
        }
    }

    /**
     * Inform the gradient about the partitioning update and let it handle it.
     */
    public static class ApplyPartitioningUpdate extends Event {

        private final LinkedList<PartitionHelper.PartitionInfo> partitionUpdate;

        public ApplyPartitioningUpdate(LinkedList<PartitionHelper.PartitionInfo> partitionInfo) {
            this.partitionUpdate = partitionInfo;
        }

        public LinkedList<PartitionHelper.PartitionInfo> getPartitionUpdates() {
            return this.partitionUpdate;
        }

    }

    public static class NumberOfShardsRequest implements KompicsEvent {

        public UUID requestId;

        public NumberOfShardsRequest(UUID requestId) {
            this.requestId = requestId;
        }
    }

    public static class NumberOfShardsResponse implements KompicsEvent {

        public UUID requestId;
        public int numShards;

        public NumberOfShardsResponse(UUID requestId, int numShards) {
            this.requestId = requestId;
            this.numShards = numShards;
        }
    }

    public static class AddIndexEntryRequest extends Event {

        private final IndexEntry entry;
        private final UUID timeoutId;

        public AddIndexEntryRequest(IndexEntry entry, UUID timeoutId) {

            this.entry = entry;
            this.timeoutId = timeoutId;
        }

        public IndexEntry getEntry() {
            return entry;
        }

        public UUID getTimeoutId() {
            return timeoutId;
        }
    }

    public static class IndexHashExchangeRequest extends Event {

        private final long lowestMissingIndexEntry;
        private final Long[] existingEntries;
        private final UUID timeoutId;
        private final int numberOfRequests;

        public IndexHashExchangeRequest(long lowestMissingIndexEntry, Long[] existingEntries, UUID timeoutId, int numberOfRequests) {
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

        public UUID getTimeoutId() {
            return timeoutId;
        }

        public int getNumberOfRequests() {
            return numberOfRequests;
        }
    }

    public static class IndexHashExchangeResponse implements KompicsEvent {

        public final List<KAddress> nodesSelectedForExchange;

        public IndexHashExchangeResponse(Collection<KAddress> nodes) {
            this.nodesSelectedForExchange = new ArrayList<>(nodes);
        }
    }

    public static class SearchRequest implements KompicsEvent {

        private final SearchPattern pattern;
        private final UUID timeoutId;
        private final int queryTimeout;
        private final Integer fanoutParameter;

        public SearchRequest(SearchPattern pattern, UUID timeoutId, int queryTimeout, Integer fanoutParameter) {

            this.pattern = pattern;
            this.timeoutId = timeoutId;
            this.queryTimeout = queryTimeout;
            this.fanoutParameter = fanoutParameter;
        }

        public Integer getFanoutParameter() {
            return fanoutParameter;
        }

        public SearchPattern getPattern() {
            return pattern;
        }

        public UUID getTimeoutId() {
            return timeoutId;
        }

        public int getQueryTimeout() {
            return queryTimeout;
        }
    }

}
