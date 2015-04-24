package se.sics.ms.util;

import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.kompics.timer.Timeout;

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
     * Timeout for the prepare phase started by the index entry addition mechanism.
     */
    public static  class EntryPrepareResponseTimeout extends Timeout{

        public final UUID entryAdditionRoundId;
        
        public EntryPrepareResponseTimeout(ScheduleTimeout request, UUID roundId) {
            super(request);
            this.entryAdditionRoundId = roundId;
        }
        
        public UUID getEntryAdditionRoundId(){
            return this.entryAdditionRoundId;
        }
    }



    
    
                
}
