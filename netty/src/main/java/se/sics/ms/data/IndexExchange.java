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
