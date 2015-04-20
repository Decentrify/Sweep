package se.sics.ms.data;

import se.sics.gvod.timer.TimeoutId;
import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.ms.types.OverlayId;
import se.sics.ms.util.PartitionHelper;

import java.util.UUID;

/**
 * Containaer class for the information exchange during the 
 * partitioning protocol.
 * 
 * Created by babbarshaer on 2015-04-20.
 */
public class PartitionPrepare {
    
    
    
    public static class Request{
        
        private final UUID partitionPrepareRoundId;
        private final PartitionHelper.PartitionInfo partitionInfo;
        private final OverlayId overlayId;
        
        public Request(UUID partitionPrepareRoundId, PartitionHelper.PartitionInfo partitionInfo, OverlayId overlayId){
            
            this.partitionPrepareRoundId = partitionPrepareRoundId;
            this.partitionInfo = partitionInfo;
            this.overlayId = overlayId;
        }

        public UUID getPartitionPrepareRoundId() {
            return partitionPrepareRoundId;
        }

        public PartitionHelper.PartitionInfo getPartitionInfo() {
            return partitionInfo;
        }

        public OverlayId getOverlayId() {
            return overlayId;
        }
    }
    
    
    public static class Response{

        private final UUID partitionPrepareRoundId;
        private final UUID partitionRequestId;
        
        public Response(UUID partitionPrepareRoundId, UUID partitionRequestId){
            this.partitionPrepareRoundId = partitionPrepareRoundId;
            this.partitionRequestId = partitionRequestId;
        }

        public UUID getPartitionPrepareRoundId() {
            return partitionPrepareRoundId;
        }

        public UUID getPartitionRequestId() {
            return partitionRequestId;
        }
        
    }

    
    public static class Timeout extends se.sics.kompics.timer.Timeout{

        public Timeout(ScheduleTimeout request) {
            super(request);
        }
    }
    
}
