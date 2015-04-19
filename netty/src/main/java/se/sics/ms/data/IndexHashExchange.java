package se.sics.ms.data;

import se.sics.ms.types.IndexHash;

import java.util.Collection;
import java.util.UUID;

/**
 * Container class for the information contained in the index hash exchange protocol.
 * Created by babbarshaer on 2015-04-19.
 */
public class IndexHashExchange {
    
    
    public static class Request {
        
        private final UUID exchangeRoundId;
        private final long lowestMissingIndexEntry;
        private final Long[] entries;
        
        public Request(UUID exchangeRoundId, long lowestMissingIndexEntry, Long[] entries){
            this.exchangeRoundId = exchangeRoundId;
            this.lowestMissingIndexEntry = lowestMissingIndexEntry;
            this.entries = entries;
        }

        public UUID getExchangeRoundId() {
            return exchangeRoundId;
        }

        public long getLowestMissingIndexEntry() {
            return lowestMissingIndexEntry;
        }

        public Long[] getEntries() {
            return entries;
        }
    }
    
        
    public static class Response {
        
        private final Collection<IndexHash> indexHashes;
        
        public Response(Collection<IndexHash>indexHashes){
            this.indexHashes = indexHashes;
        }

        public Collection<IndexHash> getIndexHashes() {
            return indexHashes;
        }
    }
    
    
}
