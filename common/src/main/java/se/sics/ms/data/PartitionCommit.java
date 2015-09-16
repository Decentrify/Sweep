package se.sics.ms.data;

import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.kompics.timer.Timeout;

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

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Request)) return false;

            Request request = (Request) o;

            if (partitionCommitTimeout != null ? !partitionCommitTimeout.equals(request.partitionCommitTimeout) : request.partitionCommitTimeout != null)
                return false;
            if (partitionRequestId != null ? !partitionRequestId.equals(request.partitionRequestId) : request.partitionRequestId != null)
                return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = partitionCommitTimeout != null ? partitionCommitTimeout.hashCode() : 0;
            result = 31 * result + (partitionRequestId != null ? partitionRequestId.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "Request{" +
                    "partitionCommitTimeout=" + partitionCommitTimeout +
                    ", partitionRequestId=" + partitionRequestId +
                    '}';
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

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Response)) return false;

            Response response = (Response) o;

            if (partitionCommitTimeout != null ? !partitionCommitTimeout.equals(response.partitionCommitTimeout) : response.partitionCommitTimeout != null)
                return false;
            if (partitionRequestId != null ? !partitionRequestId.equals(response.partitionRequestId) : response.partitionRequestId != null)
                return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = partitionCommitTimeout != null ? partitionCommitTimeout.hashCode() : 0;
            result = 31 * result + (partitionRequestId != null ? partitionRequestId.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "Response{" +
                    "partitionCommitTimeout=" + partitionCommitTimeout +
                    ", partitionRequestId=" + partitionRequestId +
                    '}';
        }

        public UUID getPartitionCommitTimeout() {
            return partitionCommitTimeout;
        }

        public UUID getPartitionRequestId() {
            return partitionRequestId;
        }
        
        
    }



    public static class Timeout extends se.sics.kompics.timer.Timeout{

        public Timeout(ScheduleTimeout scheduleTimeout){
            super(scheduleTimeout);
        }

    }
    
}
