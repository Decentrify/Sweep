package se.sics.p2ptoolbox.election.core.util;

import se.sics.gvod.timer.ScheduleTimeout;
import se.sics.gvod.timer.Timeout;

import java.util.UUID;

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
    
    public static class LeaseCommitResponseTimeout extends Timeout{
        
        public LeaseCommitResponseTimeout(ScheduleTimeout request){
            super(request);
        }
    }
    
    //Election Follower.
    
    public static class AwaitLeaseCommitTimeout extends Timeout {

        public UUID electionRoundId;
        
        public AwaitLeaseCommitTimeout(ScheduleTimeout request, UUID electionRoundId) {
            super(request);
            this.electionRoundId = electionRoundId;
        }
    }
    
    
    
}
