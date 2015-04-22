package se.sics.ms.gradient.ports;

import se.sics.gvod.net.VodAddress;
import se.sics.gvod.timer.TimeoutId;
import se.sics.kompics.Event;
import se.sics.kompics.KompicsEvent;
import se.sics.kompics.PortType;
import se.sics.ms.gradient.control.ControlMessageInternal;
import se.sics.ms.gradient.events.*;
import se.sics.ms.messages.PartitionMessage;
import se.sics.ms.types.IndexEntry;
import se.sics.ms.types.SearchPattern;
import se.sics.ms.util.PartitionHelper;
import se.sics.p2ptoolbox.util.network.impl.DecoratedAddress;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.UUID;

public class GradientRoutingPort extends PortType {
	{
		negative(AddIndexEntryRequest.class);
        negative(IndexHashExchangeRequest.class);
        positive(IndexHashExchangeResponse.class);
        negative(SearchRequest.class);
        negative(ReplicationPrepareCommitRequest.class);
        negative(ReplicationCommit.class);
        negative(SearchRequest.class);
        negative(ViewSizeMessage.Request.class);
        positive(ViewSizeMessage.Response.class);
        negative(PartitionMessage.class);
        positive(RemoveEntriesNotFromYourPartition.class);
        positive(NumberOfPartitions.class);

        // Two Phase Commit Partitioning Messages.
        negative(ApplyPartitioningUpdate.class);
        negative(LeaderGroupInformation.Request.class);
        positive(LeaderGroupInformation.Response.class);

        // Generic Pull Based Control Message Exchange.
        negative(InitiateControlMessageExchangeRound.class);
        negative(ControlMessageInternal.Request.class);
        positive(ControlMessageInternal.Response.class);

        negative(CheckPartitionInfo.Request.class);
        positive(CheckPartitionInfo.Response.class);
	}


    /**
     * Simply inform the gradient component to begin the control message exchange.
     */
    public static class InitiateControlMessageExchangeRound extends Event{
        private UUID roundId;
        private int controlMessageExchangeNumber;

        public InitiateControlMessageExchangeRound(UUID roundId , int controlMessageExchangeNumber){
            this.roundId = roundId;
            this.controlMessageExchangeNumber = controlMessageExchangeNumber;
        }

        public UUID getRoundId(){
            return this.roundId;
        }

        public int getControlMessageExchangeNumber(){
            return this.controlMessageExchangeNumber;
        }
    }


    /**
     * Inform the gradient about the partitioning update and let it handle it.
     */
    public static class ApplyPartitioningUpdate extends Event{

        private final LinkedList<PartitionHelper.PartitionInfo> partitionUpdate;

        public ApplyPartitioningUpdate(LinkedList<PartitionHelper.PartitionInfo> partitionInfo){
            this.partitionUpdate =partitionInfo;
        }

        public LinkedList<PartitionHelper.PartitionInfo> getPartitionUpdates (){
            return this.partitionUpdate;
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

        private HashSet<DecoratedAddress> nodesSelectedForExchange;

        public IndexHashExchangeResponse(Collection<DecoratedAddress> nodes) {
            this.nodesSelectedForExchange = new HashSet<DecoratedAddress>(nodes);
        }

        public HashSet<DecoratedAddress> getNodesSelectedForExchange() {
            return nodesSelectedForExchange;
        }
    }

    public static class SearchRequest extends Event {
        private final SearchPattern pattern;
        private final UUID timeoutId;
        private final int queryTimeout;

        public SearchRequest(SearchPattern pattern, UUID timeoutId, int queryTimeout) {
            this.pattern = pattern;
            this.timeoutId = timeoutId;
            this.queryTimeout = queryTimeout;
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
