package se.sics.ms.data;

import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.kompics.timer.Timeout;
import se.sics.ms.types.SearchDescriptor;

import java.util.List;
import java.util.UUID;

/**
 * Container for the content being exchanged between the Request / Response 
 * Messages as part of  
 *
 * Created by babbarshaer on 2015-04-18.
 */
public class LeaderLookup {

    public static final int QueryLimit = 4;
    public static final int ResponseLimit = 8;
    
    
    public static class Request {
        
        public final UUID leaderLookupRound;
        
        public Request(UUID leaderLookupRound){
            this.leaderLookupRound = leaderLookupRound;
        }
        
        
    }
    
    public static class Response {
        
        private final UUID leaderLookupRound;
        private final boolean leader;
        private final List<SearchDescriptor> searchDescriptors;
        
        public Response (UUID leaderLookupRound, boolean leader, List<SearchDescriptor> searchDescriptors){
            
            this.leaderLookupRound = leaderLookupRound;
            this.leader = leader;
            this.searchDescriptors = searchDescriptors;
        }


        public UUID getLeaderLookupRound() {
            return leaderLookupRound;
        }

        public boolean isLeader() {
            return leader;
        }

        public List<SearchDescriptor> getSearchDescriptors() {
            return searchDescriptors;
        }
    }
    
    public static class Timeout extends se.sics.kompics.timer.Timeout{

        public Timeout(ScheduleTimeout request) {
            super(request);
        }
    }
    
}
