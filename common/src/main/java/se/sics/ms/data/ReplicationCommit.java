package se.sics.ms.data;

import java.util.UUID;

/**
 * Container for the information exchanged as part of the commit part of the index entry addition protocol.
 * Currently the commit is only a one way message of letting the nodes know about the index entry to be committed.
 *
 * <b>TO DO:</b> need to update the protocol for exact commit of the index entry and a retry mechanism for the same.
 * Created by babbar on 2015-04-20.
 */
public class ReplicationCommit {


    public static class Request {

        private UUID commitRoundId;
        private final long entryId;
        private final String signature;

        public Request(UUID commitRoundId, long entryId, String signature){
            this.commitRoundId = commitRoundId;
            this.entryId = entryId;
            this.signature = signature;
        }

        public UUID getCommitRoundId(){
            return this.commitRoundId;
        }

        public long getEntryId() {
            return entryId;
        }

        public String getSignature() {
            return signature;
        }


        @Override
        public boolean equals(Object o) {

            if (this == o) return true;
            if (!(o instanceof Request)) return false;

            Request request = (Request) o;

            if (entryId != request.entryId) return false;
            if (commitRoundId != null ? !commitRoundId.equals(request.commitRoundId) : request.commitRoundId != null)
                return false;
            if (signature != null ? !signature.equals(request.signature) : request.signature != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = commitRoundId != null ? commitRoundId.hashCode() : 0;
            result = 31 * result + (int) (entryId ^ (entryId >>> 32));
            result = 31 * result + (signature != null ? signature.hashCode() : 0);
            return result;
        }


        @Override
        public String toString() {
            return "Request{" +
                    "commitRoundId=" + commitRoundId +
                    ", entryId=" + entryId +
                    ", signature='" + signature + '\'' +
                    '}';
        }
    }


}
