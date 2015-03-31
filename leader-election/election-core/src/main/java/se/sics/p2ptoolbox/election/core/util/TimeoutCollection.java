package se.sics.p2ptoolbox.election.core.util;

import se.sics.gvod.timer.ScheduleTimeout;
import se.sics.gvod.timer.Timeout;

/**
 * Created by babbarshaer on 2015-03-31.
 */
public class TimeoutCollection {
    
    // Common.
    public static class LeaseTimeout extends Timeout{
        
        public LeaseTimeout(ScheduleTimeout request) {
            super(request);
        }
    }
    
    // Election Leader.
    public static class PromiseRoundTimeout extends Timeout{

        public PromiseRoundTimeout(ScheduleTimeout request) {
            super(request);
        }
    }
    
    
    //Election Follower.
    
    public static class AwaitLeaseCommitTimeout extends Timeout {

        public AwaitLeaseCommitTimeout(ScheduleTimeout request) {
            super(request);
        }
    }
    
    
    
}
