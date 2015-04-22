package se.sics.ms.data;

import se.sics.ms.types.Id;
import se.sics.ms.types.IndexEntry;

import java.util.Collection;
import java.util.UUID;

/**
 * Container for the information exchanged between the nodes during the 
 * index exchange protocol.
 *
 * Created by babbarshaer on 2015-04-20.
 */
public class IndexExchange {
    
    public static class Request {
        
        private final UUID exchangeRoundId;
        private final Collection<Id> ids;
        
        public Request(UUID exchangeRoundId, Collection<Id> ids){
            this.exchangeRoundId = exchangeRoundId;
            this.ids = ids;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Request)) return false;

            Request request = (Request) o;

            if (exchangeRoundId != null ? !exchangeRoundId.equals(request.exchangeRoundId) : request.exchangeRoundId != null)
                return false;
            if (ids != null ? !ids.equals(request.ids) : request.ids != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = exchangeRoundId != null ? exchangeRoundId.hashCode() : 0;
            result = 31 * result + (ids != null ? ids.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "Request{" +
                    "exchangeRoundId=" + exchangeRoundId +
                    ", ids=" + ids +
                    '}';
        }

        public UUID getExchangeRoundId() {
            return exchangeRoundId;
        }

        public Collection<Id> getIds() {
            return ids;
        }
    }
    
    
    public static class Response {

        public static final int MAX_RESULTS_STR_LEN = 1400;

        private final UUID exchangeRoundId;
        private final Collection<IndexEntry> indexEntries;
        private final int numResponses;
        private final int responseNumber;


        public Response (UUID exchangeRoundId, Collection<IndexEntry> indexEntries, int numResponses, int responseNumber){
            
            this.exchangeRoundId = exchangeRoundId;
            this.indexEntries = indexEntries;
            this.numResponses = numResponses;
            this.responseNumber = responseNumber;
            
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Response)) return false;

            Response response = (Response) o;

            if (numResponses != response.numResponses) return false;
            if (responseNumber != response.responseNumber) return false;
            if (exchangeRoundId != null ? !exchangeRoundId.equals(response.exchangeRoundId) : response.exchangeRoundId != null)
                return false;
            if (indexEntries != null ? !indexEntries.equals(response.indexEntries) : response.indexEntries != null)
                return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = exchangeRoundId != null ? exchangeRoundId.hashCode() : 0;
            result = 31 * result + (indexEntries != null ? indexEntries.hashCode() : 0);
            result = 31 * result + numResponses;
            result = 31 * result + responseNumber;
            return result;
        }


        @Override
        public String toString() {
            return "Response{" +
                    "exchangeRoundId=" + exchangeRoundId +
                    ", indexEntries=" + indexEntries +
                    ", numResponses=" + numResponses +
                    ", responseNumber=" + responseNumber +
                    '}';
        }

        public UUID getExchangeRoundId() {
            return exchangeRoundId;
        }

        public Collection<IndexEntry> getIndexEntries() {
            return indexEntries;
        }

        public int getNumResponses() {
            return numResponses;
        }

        public int getResponseNumber() {
            return responseNumber;
        }
        
    }
}
