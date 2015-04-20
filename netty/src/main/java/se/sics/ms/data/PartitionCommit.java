package se.sics.ms.data;

import java.util.UUID;

/**
 * Container class for the information exchanged during the partition commit phase
 * of the partitioning protocol.
 *  
 * Created by babbarshaer on 2015-04-20.
 */
public class PartitionCommit {
    
    public static class Request {
        
        private final UUID partitionCommitTimeout;
        private final UUID partitionRequestId;
        
        public Request(UUID partitionCommitTimeout, UUID partitionRequestId){
            
            this.partitionCommitTimeout = partitionCommitTimeout;
            this.partitionRequestId = partitionRequestId;
        }

        public UUID getPartitionCommitTimeout() {
            return partitionCommitTimeout;
        }

        public UUID getPartitionRequestId() {
            return partitionRequestId;
        }
    }

    public static class Response {

        private final UUID partitionCommitTimeout;
        private final UUID partitionRequestId;

        public Response (UUID partitionCommitTimeout, UUID partitionRequestId){

            this.partitionCommitTimeout = partitionCommitTimeout;
            this.partitionRequestId = partitionRequestId;
        }

        public UUID getPartitionCommitTimeout() {
            return partitionCommitTimeout;
        }

        public UUID getPartitionRequestId() {
            return partitionRequestId;
        }
        
        
    }
    
}
