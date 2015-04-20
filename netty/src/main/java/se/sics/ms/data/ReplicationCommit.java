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
    }


}
