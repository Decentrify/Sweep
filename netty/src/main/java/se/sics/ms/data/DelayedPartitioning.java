package se.sics.ms.data;

import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.kompics.timer.Timeout;
import se.sics.ms.util.PartitionHelper;

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

/**
 * Container for the information requested as part of Request / Response for the partitioning 
 * information request once the partition hashes have matched.
 *  
 * Created by babbarshaer on 2015-04-20.
 */
public class DelayedPartitioning {
    
    
    public static class Request{
        
        private final UUID roundId;
        private final List<UUID> partitionRequestIds;
        
        public Request( UUID roundId, List<UUID> partitionRequestIds){
            this.roundId = roundId;
            this.partitionRequestIds = partitionRequestIds;
            
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Request)) return false;

            Request request = (Request) o;

            if (partitionRequestIds != null ? !partitionRequestIds.equals(request.partitionRequestIds) : request.partitionRequestIds != null)
                return false;
            if (roundId != null ? !roundId.equals(request.roundId) : request.roundId != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = roundId != null ? roundId.hashCode() : 0;
            result = 31 * result + (partitionRequestIds != null ? partitionRequestIds.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "Request{" +
                    "roundId=" + roundId +
                    ", partitionRequestIds=" + partitionRequestIds +
                    '}';
        }

        public UUID getRoundId() {
            return roundId;
        }

        public List<UUID> getPartitionRequestIds() {
            return partitionRequestIds;
        }
    }
    
    
    
    public static class Response{

        private final UUID roundId;
        private final LinkedList<PartitionHelper.PartitionInfo> partitionHistory;
        
        public Response(UUID roundId, LinkedList<PartitionHelper.PartitionInfo> partitionHistory){
            this.roundId = roundId;
            this.partitionHistory = partitionHistory;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Response)) return false;

            Response response = (Response) o;

            if (partitionHistory != null ? !partitionHistory.equals(response.partitionHistory) : response.partitionHistory != null)
                return false;
            if (roundId != null ? !roundId.equals(response.roundId) : response.roundId != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = roundId != null ? roundId.hashCode() : 0;
            result = 31 * result + (partitionHistory != null ? partitionHistory.hashCode() : 0);
            return result;
        }

        @Override
        public String
        toString() {
            return "Response{" +
                    "roundId=" + roundId +
                    ", partitionHistory=" + partitionHistory +
                    '}';
        }

        public UUID getRoundId() {
            return roundId;
        }

        public LinkedList<PartitionHelper.PartitionInfo> getPartitionHistory() {
            return partitionHistory;
        }
        
    }
    
    
    
    public static class Timeout extends se.sics.kompics.timer.Timeout{

        public Timeout(ScheduleTimeout request) {
            super(request);
        }
    }
    
}
