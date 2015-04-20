package se.sics.ms.data;

import se.sics.gvod.net.VodAddress;
import se.sics.gvod.timer.TimeoutId;
import se.sics.ms.exceptions.IllegalSearchString;
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

        public Response(UUID searchTimeoutId, Collection<IndexEntry> results, int partitionId , int numResponses, int responseNumber) throws IllegalSearchString {
            
            this.partitionId = partitionId;
            this.numResponses = numResponses;
            this.responseNumber = responseNumber;
            this.results = results;
            this.searchTimeoutId = searchTimeoutId;
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
}
