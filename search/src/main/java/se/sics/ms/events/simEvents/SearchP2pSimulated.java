package se.sics.ms.events.simEvents;

import se.sics.ms.types.SearchPattern;

/**
 * Simulation event for searching in the system.
 *
 * Created by babbarshaer on 2015-04-25.
 */
public class SearchP2pSimulated {
    
    
    public static class Request {

        private final SearchPattern searchPattern;
        private final Integer searchTimeout;
        private final Integer fanoutParameter;

        public Request(SearchPattern pattern, Integer searchTimeout, Integer fanoutParameter){
            this.searchPattern = pattern;
            this.searchTimeout  = searchTimeout;
            this.fanoutParameter = fanoutParameter;
        }

        @Override
        public boolean equals(Object o) {

            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Request that = (Request) o;

            if (searchPattern != null ? !searchPattern.equals(that.searchPattern) : that.searchPattern != null)
                return false;

            return true;
        }

        @Override
        public int hashCode() {
            return searchPattern != null ? searchPattern.hashCode() : 0;
        }


        public Integer getSearchTimeout() {
            return searchTimeout;
        }

        public Integer getFanoutParameter() {
            return fanoutParameter;
        }

        public SearchPattern getSearchPattern() {
            return searchPattern;
        }
            
    }
    
    
    
    public static class Response {
        
        public int responses;
        public int partitionHit;

        public Response(int responses, int partitionHit){
            this.responses = responses;
            this.partitionHit = partitionHit;
        }
        
        public int getResponses() {
            return responses;
        }

        public int getPartitionHit() {
            return partitionHit;
        }
    }
    
    
}
