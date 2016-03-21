package se.sics.ms.data;

import se.sics.ms.types.ApplicationEntry;

import java.util.UUID;

/**
 * Container for the information exchanged as part of the commit process of the index entry addition process.
 * 
 * Created by babbarshaer on 2015-05-06.
 */
public class EntryAddCommit {

    public static class Request {

        private UUID commitRoundId;
        private final ApplicationEntry.ApplicationEntryId entryId;
        private final String signature;

        public Request(UUID commitRoundId, ApplicationEntry.ApplicationEntryId entryId, String signature) {
            this.commitRoundId = commitRoundId;
            this.entryId = entryId;
            this.signature = signature;
        }


        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Request request = (Request) o;

            if (commitRoundId != null ? !commitRoundId.equals(request.commitRoundId) : request.commitRoundId != null)
                return false;
            if (entryId != null ? !entryId.equals(request.entryId) : request.entryId != null) return false;
            if (signature != null ? !signature.equals(request.signature) : request.signature != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = commitRoundId != null ? commitRoundId.hashCode() : 0;
            result = 31 * result + (entryId != null ? entryId.hashCode() : 0);
            result = 31 * result + (signature != null ? signature.hashCode() : 0);
            return result;
        }

        
        
        
        public UUID getCommitRoundId() {
            return commitRoundId;
        }

        public ApplicationEntry.ApplicationEntryId getEntryId() {
            return entryId;
        }

        public String getSignature() {
            return signature;
        }
    }
    
}
