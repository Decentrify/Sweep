package se.sics.ms.data;

import se.sics.ms.exceptions.IllegalSearchString;
import se.sics.ms.types.ApplicationEntry;
import se.sics.ms.types.IndexEntry;
import se.sics.ms.types.SearchPattern;

import java.util.Collection;
import java.util.UUID;

/**
 * Content Wrapper for the information exchanged during the search protocols.
 * 
 * Created by babbarshaer on 2015-04-19.
 */
public class SearchInfo {
    
    
    public static class Request{
        
        private final UUID requestId;
        private final int partitionId;
        private final SearchPattern pattern;
        
        public Request(UUID requestId, int partitionId, SearchPattern pattern){
            this.requestId = requestId;
            this.partitionId = partitionId;
            this.pattern = pattern;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Request)) return false;

            Request request = (Request) o;

            if (partitionId != request.partitionId) return false;
            if (pattern != null ? !pattern.equals(request.pattern) : request.pattern != null) return false;
            if (requestId != null ? !requestId.equals(request.requestId) : request.requestId != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = requestId != null ? requestId.hashCode() : 0;
            result = 31 * result + partitionId;
            result = 31 * result + (pattern != null ? pattern.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "Request{" +
                    "requestId=" + requestId +
                    ", partitionId=" + partitionId +
                    ", pattern=" + pattern +
                    '}';
        }

        public SearchPattern getPattern() {
            return pattern;
        }

        public UUID getRequestId() {
            return requestId;
        }

        public int getPartitionId() {
            return partitionId;
        }
    }
    
    public static class Response{
        
        public static final int MAX_RESULTS_STR_LEN = 1400;

        private final Collection<IndexEntry> results;
        private final int numResponses;
        private final int responseNumber;
        private final UUID searchTimeoutId;
        private final int partitionId;

        public Response(UUID searchTimeoutId, Collection<IndexEntry> results, int partitionId , int numResponses, int responseNumber) {
            
            this.partitionId = partitionId;
            this.numResponses = numResponses;
            this.responseNumber = responseNumber;
            this.results = results;
            this.searchTimeoutId = searchTimeoutId;
        }


        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Response)) return false;

            Response response = (Response) o;

            if (numResponses != response.numResponses) return false;
            if (partitionId != response.partitionId) return false;
            if (responseNumber != response.responseNumber) return false;
            if (results != null ? !results.equals(response.results) : response.results != null) return false;
            if (searchTimeoutId != null ? !searchTimeoutId.equals(response.searchTimeoutId) : response.searchTimeoutId != null)
                return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = results != null ? results.hashCode() : 0;
            result = 31 * result + numResponses;
            result = 31 * result + responseNumber;
            result = 31 * result + (searchTimeoutId != null ? searchTimeoutId.hashCode() : 0);
            result = 31 * result + partitionId;
            return result;
        }

        @Override
        public String toString() {
            return "Response{" +
                    "results=" + results +
                    ", numResponses=" + numResponses +
                    ", responseNumber=" + responseNumber +
                    ", searchTimeoutId=" + searchTimeoutId +
                    ", partitionId=" + partitionId +
                    '}';
        }

        public Collection<IndexEntry> getResults() {
            return results;
        }

        public int getNumResponses() {
            return numResponses;
        }

        public int getResponseNumber() {
            return responseNumber;
        }

        public UUID getSearchTimeoutId() {
            return searchTimeoutId;
        }

        public int getPartitionId() {
            return partitionId;
        }

    }
 
    
    public static class ResponseUpdated {

        private final Collection<ApplicationEntry> results;
        private final int numResponses;
        private final int responseNumber;
        private final UUID searchTimeoutId;
        private final int partitionId;

        public ResponseUpdated ( UUID searchTimeoutId, Collection<ApplicationEntry> results, int partitionId , int numResponses, int responseNumber ) {

            this.partitionId = partitionId;
            this.numResponses = numResponses;
            this.responseNumber = responseNumber;
            this.results = results;
            this.searchTimeoutId = searchTimeoutId;
        }

        
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ResponseUpdated that = (ResponseUpdated) o;

            if (numResponses != that.numResponses) return false;
            if (partitionId != that.partitionId) return false;
            if (responseNumber != that.responseNumber) return false;
            if (results != null ? !results.equals(that.results) : that.results != null) return false;
            if (searchTimeoutId != null ? !searchTimeoutId.equals(that.searchTimeoutId) : that.searchTimeoutId != null)
                return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = results != null ? results.hashCode() : 0;
            result = 31 * result + numResponses;
            result = 31 * result + responseNumber;
            result = 31 * result + (searchTimeoutId != null ? searchTimeoutId.hashCode() : 0);
            result = 31 * result + partitionId;
            return result;
        }

        @Override
        public String toString() {
            return "ResponseUpdated{" +
                    "results=" + results +
                    ", numResponses=" + numResponses +
                    ", responseNumber=" + responseNumber +
                    ", searchTimeoutId=" + searchTimeoutId +
                    ", partitionId=" + partitionId +
                    '}';
        }

        public Collection<ApplicationEntry> getResults() {
            return results;
        }

        public int getNumResponses() {
            return numResponses;
        }

        public int getResponseNumber() {
            return responseNumber;
        }

        public UUID getSearchTimeoutId() {
            return searchTimeoutId;
        }

        public int getPartitionId() {
            return partitionId;
        }
    }
    
}
