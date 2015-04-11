package se.sics.ms.util;

import se.sics.gvod.timer.SchedulePeriodicTimeout;
import se.sics.gvod.timer.ScheduleTimeout;
import se.sics.gvod.timer.Timeout;
import se.sics.ms.timeout.IndividualTimeout;

/**
 * Collection of Timeouts to be used by the search during different phases of the
 * protocol.
 * 
 * Created by babbarshaer on 2015-04-11.
 */
public class TimeoutCollection {


    public static class ExchangeRound extends IndividualTimeout {

        public ExchangeRound(SchedulePeriodicTimeout request, int id) {
            super(request, id);
        }
    }

    // Control Message Exchange Round.
    public static class ControlMessageExchangeRound extends IndividualTimeout {

        public ControlMessageExchangeRound(SchedulePeriodicTimeout request, int id) {
            super(request, id);
        }
    }

    public static class SearchTimeout extends IndividualTimeout {

        public SearchTimeout(ScheduleTimeout request, int id) {
            super(request, id);
        }
    }

    public static class IndexExchangeTimeout extends IndividualTimeout {

        public IndexExchangeTimeout(ScheduleTimeout request, int id) {
            super(request, id);
        }
    }

    /**
     * Periodic scheduled timeout event to garbage collect the recent request
     * data structure of {@link se.sics.ms.search.Search}.
     */
    public static class RecentRequestsGcTimeout extends IndividualTimeout {

        public RecentRequestsGcTimeout(SchedulePeriodicTimeout request, int id) {
            super(request, id);
        }
    }
    
    /**
     * Timeout for the prepare phase started by the index entry addition mechanism.
     */
    public static  class EntryPrepareResponseTimeout extends Timeout{

        public EntryPrepareResponseTimeout(ScheduleTimeout request) {
            super(request);
        }
    }



    
    
                
}
