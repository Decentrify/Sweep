package se.sics.ms.util;

import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.kompics.timer.Timeout;
import se.sics.ms.types.ApplicationEntry;
import se.sics.ms.types.LeaderUnit;

import java.util.UUID;

/**
 * Collection of Timeouts to be used by the search during different phases of the
 * protocol.
 * 
 * Created by babbarshaer on 2015-04-11.
 */
public class TimeoutCollection {


    public static class ExchangeRound extends Timeout {

        public ExchangeRound(SchedulePeriodicTimeout request) {
            super(request);
        }
    }


    public static class EntryExchangeRound extends Timeout{

        public EntryExchangeRound(SchedulePeriodicTimeout request) {
            super(request);
        }
    }


    // Control Message Exchange Round.
    public static class ControlMessageExchangeRound extends Timeout {

        public ControlMessageExchangeRound(SchedulePeriodicTimeout request) {
            super(request);
        }
    }

    public static class SearchTimeout extends Timeout {

        public SearchTimeout(se.sics.kompics.timer.ScheduleTimeout request) {
            super(request);
        }
    }

    public static class IndexExchangeTimeout extends Timeout {

        public IndexExchangeTimeout(ScheduleTimeout request) {
            super(request);
        }
    }

    /**
     * Periodic scheduled timeout event to garbage collect the recent request
     * data structure of {@link se.sics.ms.search.Search}.
     */
    public static class RecentRequestsGcTimeout extends Timeout {

        public RecentRequestsGcTimeout(SchedulePeriodicTimeout request) {
            super(request);
        }
    }
    
    /**
     * Timeout for the prepare phase started 
     * by the index entry addition mechanism.
     */
    public static  class EntryPrepareResponseTimeout extends Timeout{

        public final UUID entryAdditionRoundId;
        
        public EntryPrepareResponseTimeout( ScheduleTimeout request, UUID roundId) {
            super(request);
            this.entryAdditionRoundId = roundId;
        }
        
        public UUID getEntryAdditionRoundId(){
            return this.entryAdditionRoundId;
        }
    }


    /**
     * Timeout which gets triggered when a node is 
     * trying to add landing entry in the system and the entry add
     * doesn't complete on time.
     */
    public static class LandingEntryAddTimeout extends Timeout{

        public LandingEntryAddTimeout(ScheduleTimeout request) {
            super(request);
        }
    }


    /**
     * Timeout which gets triggered when a node is trying to 
     * add a new epoch in the system and the round doesn't gets completed 
     * on time.
     */
    public static class EpochAdditionTimeout extends Timeout{

        public LeaderUnit epochUpdate;
        public LeaderUnit previousUpdate;
        
        public EpochAdditionTimeout(ScheduleTimeout request, LeaderUnit previousUpdate, LeaderUnit epochUpdate) {
            super(request);
            this.epochUpdate = epochUpdate;
            this.previousUpdate = previousUpdate;
        }
        
    }


    /**
     * Shard round gets initiated when a leader node thinks that
     * the number of entries have exceeded the total capacity of the shard and therefore
     * the size of shard need to shrink and therefore initiate the sharding protocol.
     */
    public static class ShardRoundTimeout extends Timeout{

        public LeaderUnit previousUpdate;
        public LeaderUnit shardUpdate;

        public ShardRoundTimeout(ScheduleTimeout request, LeaderUnit previousUpdate, LeaderUnit shardUpdate) {
            super(request);
            this.shardUpdate = shardUpdate;
            this.previousUpdate = previousUpdate;
        }
        
    }


    /**
     * As part of the epoch addition protocol,
     * the node which the leader contacts keeps the epoch to be added till the timeout
     * gets triggered and therefore removes the buffered entry from the system.
     */
    public static class AwaitingEpochCommit extends Timeout {

        public UUID epochAddRoundID;
        
        public AwaitingEpochCommit(ScheduleTimeout request, UUID epochAddRoundID) {
            super(request);
            this.epochAddRoundID = epochAddRoundID;
        }
        
    }


    /**
     * As the name suggests the node waits for the shard commit 
     * message from the leader node to confirm the shard entry commit.
     */
    public static class AwaitingShardCommit extends Timeout {

        public AwaitingShardCommit(ScheduleTimeout request) {
            super(request);
        }
    }


    /**
     * In case the leader thinks that the sharding needs to be initiated,
     * it times out for a while so that the lagging behind nodes can catch up.
     *
     */
    public static class PreShardTimeout extends Timeout {

        public ApplicationEntry.ApplicationEntryId medianId;
        
        public PreShardTimeout(ScheduleTimeout request, ApplicationEntry.ApplicationEntryId entryId) {
            super(request);
            this.medianId = entryId;
        }
    }
    
    
}
