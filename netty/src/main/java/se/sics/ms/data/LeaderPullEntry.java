package se.sics.ms.data;

import se.sics.ms.types.ApplicationEntry;

import java.util.Collection;
import java.util.UUID;

/**
 * Container for the data exchanged between the nodes as part of the 
 * a special direct pull by the node in case the leader is present in the gradient.
 *
 * Created by babbarshaer on 2015-05-18.
 */
public class LeaderPullEntry {
    
    
    public static class Request {
        
        private UUID directPullRound;
        private ApplicationEntry.ApplicationEntryId lowestMissingEntryId;
        
        public Request( UUID directPullRound, ApplicationEntry.ApplicationEntryId lowestMissingEntryId){
            
            this.directPullRound = directPullRound;
            this.lowestMissingEntryId = lowestMissingEntryId;
        }


        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Request request = (Request) o;

            if (directPullRound != null ? !directPullRound.equals(request.directPullRound) : request.directPullRound != null)
                return false;
            if (lowestMissingEntryId != null ? !lowestMissingEntryId.equals(request.lowestMissingEntryId) : request.lowestMissingEntryId != null)
                return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = directPullRound != null ? directPullRound.hashCode() : 0;
            result = 31 * result + (lowestMissingEntryId != null ? lowestMissingEntryId.hashCode() : 0);
            return result;
        }


        @Override
        public String toString() {
            return "Request{" +
                    "directPullRound=" + directPullRound +
                    ", lowestMissingEntryId=" + lowestMissingEntryId +
                    '}';
        }

        public UUID getDirectPullRound() {
            return directPullRound;
        }

        public ApplicationEntry.ApplicationEntryId getLowestMissingEntryId() {
            return lowestMissingEntryId;
        }
    }
    

    public static class Response {
        
        private UUID directPullRound;
        private Collection<ApplicationEntry> missingEntries;

        public Response(UUID directPullRound, Collection<ApplicationEntry> missingEntries){
            
            this.directPullRound = directPullRound;
            this.missingEntries = missingEntries;
        }


        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Response response = (Response) o;

            if (directPullRound != null ? !directPullRound.equals(response.directPullRound) : response.directPullRound != null)
                return false;
            if (missingEntries != null ? !missingEntries.equals(response.missingEntries) : response.missingEntries != null)
                return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = directPullRound != null ? directPullRound.hashCode() : 0;
            result = 31 * result + (missingEntries != null ? missingEntries.hashCode() : 0);
            return result;
        }


        @Override
        public String toString() {
            return "Response{" +
                    "directPullRound=" + directPullRound +
                    ", missingEntries=" + missingEntries +
                    '}';
        }

        public UUID getDirectPullRound() {
            return directPullRound;
        }

        public Collection<ApplicationEntry> getMissingEntries() {
            return missingEntries;
        }
    }
    

}
