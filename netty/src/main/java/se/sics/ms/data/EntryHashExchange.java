package se.sics.ms.data;

import se.sics.ms.types.ApplicationEntry;
import se.sics.ms.types.EntryHash;
import se.sics.ms.types.IndexHash;

import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;

/**
 * Container class for the information contained in the index hash exchange protocol.
 * Created by babbarshaer on 2015-04-19.
 */
public class EntryHashExchange {
    
    
    public static class Request {
        
        private final UUID exchangeRoundId;
        private final ApplicationEntry.ApplicationEntryId lowestMissingEntryId;
        private final Long[] entries;
        
        public Request(UUID exchangeRoundId, ApplicationEntry.ApplicationEntryId lowestMissingEntryId, Long[] entries){
            this.exchangeRoundId = exchangeRoundId;
            this.lowestMissingEntryId = lowestMissingEntryId;
            this.entries = entries;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Request)) return false;

            Request request = (Request) o;

            if (lowestMissingEntryId != request.lowestMissingEntryId) return false;
            if (!Arrays.equals(entries, request.entries)) return false;
            if (exchangeRoundId != null ? !exchangeRoundId.equals(request.exchangeRoundId) : request.exchangeRoundId != null)
                return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = exchangeRoundId != null ? exchangeRoundId.hashCode() : 0;
            result = 31 * result + (lowestMissingEntryId!= null ? lowestMissingEntryId.hashCode() : 0);
            result = 31 * result + (entries != null ? Arrays.hashCode(entries) : 0);
            return result;
        }


        @Override
        public String toString() {
            return "Request{" +
                    "exchangeRoundId=" + exchangeRoundId +
                    ", lowestMissingIndexEntry=" + lowestMissingEntryId +
                    ", entries=" + Arrays.toString(entries) +
                    '}';
        }

        public UUID getExchangeRoundId() {
            return exchangeRoundId;
        }

        public ApplicationEntry.ApplicationEntryId getLowestMissingIndexEntry() {
            return lowestMissingEntryId;
        }

        public Long[] getEntries() {
            return entries;
        }
    }
    
        
    public static class Response {
        
        private final Collection<EntryHash> entryHashes;
        private final UUID exchangeRoundId;
        
        public Response(UUID exchangeRoundId, Collection<EntryHash> entryHashes){
            this.entryHashes = entryHashes;
            this.exchangeRoundId = exchangeRoundId;
        }


        @Override
        public boolean equals(Object o) {

            if (this == o) return true;
            if (!(o instanceof Response)) return false;

            Response response = (Response) o;

            if (exchangeRoundId != null ? !exchangeRoundId.equals(response.exchangeRoundId) : response.exchangeRoundId != null)
                return false;
            if (entryHashes != null ? !entryHashes.equals(response.entryHashes) : response.entryHashes != null)
                return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = entryHashes != null ? entryHashes.hashCode() : 0;
            result = 31 * result + (exchangeRoundId != null ? exchangeRoundId.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "Response{" +
                    "indexHashes=" + entryHashes +
                    ", exchangeRoundId=" + exchangeRoundId +
                    '}';
        }

        public Collection<EntryHash> getEntryHashes() {
            return entryHashes;
        }
        
        public UUID getExchangeRoundId(){
            return this.exchangeRoundId;
        }
    }
    
    
}
