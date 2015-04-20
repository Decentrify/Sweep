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
