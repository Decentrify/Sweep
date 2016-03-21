package se.sics.ms.data;

import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.ms.types.PeerDescriptor;

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
        
        private final UUID leaderLookupRound;
        
        public Request(UUID leaderLookupRound){
            this.leaderLookupRound = leaderLookupRound;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Request)) return false;

            Request request = (Request) o;

            if (leaderLookupRound != null ? !leaderLookupRound.equals(request.leaderLookupRound) : request.leaderLookupRound != null)
                return false;

            return true;
        }


        @Override
        public String toString() {
            return "Request{" +
                    "leaderLookupRound=" + leaderLookupRound +
                    '}';
        }

        @Override
        public int hashCode() {
            return leaderLookupRound != null ? leaderLookupRound.hashCode() : 0;
        }

        public UUID getLeaderLookupRound() {
            return leaderLookupRound;
        }
    }
    
    public static class Response {
        
        private final UUID leaderLookupRound;
        private final boolean leader;
        private final List<PeerDescriptor> searchDescriptors;
        
        public Response (UUID leaderLookupRound, boolean leader, List<PeerDescriptor> searchDescriptors){
            
            this.leaderLookupRound = leaderLookupRound;
            this.leader = leader;
            this.searchDescriptors = searchDescriptors;
        }


        @Override
        public boolean equals(Object o) {

            if (this == o) return true;
            if (!(o instanceof Response)) return false;

            Response response = (Response) o;

            if (leader != response.leader) return false;
            if (leaderLookupRound != null ? !leaderLookupRound.equals(response.leaderLookupRound) : response.leaderLookupRound != null)
                return false;
            if (searchDescriptors != null ? !searchDescriptors.equals(response.searchDescriptors) : response.searchDescriptors != null)
                return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = leaderLookupRound != null ? leaderLookupRound.hashCode() : 0;
            result = 31 * result + (leader ? 1 : 0);
            result = 31 * result + (searchDescriptors != null ? searchDescriptors.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "Response{" +
                    "leaderLookupRound=" + leaderLookupRound +
                    ", leader=" + leader +
                    ", searchDescriptors=" + searchDescriptors +
                    '}';
        }

        public UUID getLeaderLookupRound() {
            return leaderLookupRound;
        }

        public boolean isLeader() {
            return leader;
        }

        public List<PeerDescriptor> getSearchDescriptors() {
            return searchDescriptors;
        }
    }
    
    public static class Timeout extends se.sics.kompics.timer.Timeout{

        public Timeout(ScheduleTimeout request) {
            super(request);
        }
    }
    
}
