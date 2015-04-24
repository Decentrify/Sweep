package se.sics.ms.data;

import se.sics.ms.types.IndexHash;

import java.util.Arrays;
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

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Request)) return false;

            Request request = (Request) o;

            if (lowestMissingIndexEntry != request.lowestMissingIndexEntry) return false;
            if (!Arrays.equals(entries, request.entries)) return false;
            if (exchangeRoundId != null ? !exchangeRoundId.equals(request.exchangeRoundId) : request.exchangeRoundId != null)
                return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = exchangeRoundId != null ? exchangeRoundId.hashCode() : 0;
            result = 31 * result + (int) (lowestMissingIndexEntry ^ (lowestMissingIndexEntry >>> 32));
            result = 31 * result + (entries != null ? Arrays.hashCode(entries) : 0);
            return result;
        }


        @Override
        public String toString() {
            return "Request{" +
                    "exchangeRoundId=" + exchangeRoundId +
                    ", lowestMissingIndexEntry=" + lowestMissingIndexEntry +
                    ", entries=" + Arrays.toString(entries) +
                    '}';
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
        private final UUID exchangeRoundId;
        
        public Response(UUID exchangeRoundId, Collection<IndexHash>indexHashes){
            this.indexHashes = indexHashes;
            this.exchangeRoundId = exchangeRoundId;
        }


        @Override
        public boolean equals(Object o) {

            if (this == o) return true;
            if (!(o instanceof Response)) return false;

            Response response = (Response) o;

            if (exchangeRoundId != null ? !exchangeRoundId.equals(response.exchangeRoundId) : response.exchangeRoundId != null)
                return false;
            if (indexHashes != null ? !indexHashes.equals(response.indexHashes) : response.indexHashes != null)
                return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = indexHashes != null ? indexHashes.hashCode() : 0;
            result = 31 * result + (exchangeRoundId != null ? exchangeRoundId.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "Response{" +
                    "indexHashes=" + indexHashes +
                    ", exchangeRoundId=" + exchangeRoundId +
                    '}';
        }

        public Collection<IndexHash> getIndexHashes() {
            return indexHashes;
        }
        
        public UUID getExchangeRoundId(){
            return this.exchangeRoundId;
        }
    }
    
    
}
