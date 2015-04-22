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


        @Override
        public boolean equals(Object o) {

            if (this == o) return true;
            if (!(o instanceof Request)) return false;

            Request request = (Request) o;

            if (overlayId != null ? !overlayId.equals(request.overlayId) : request.overlayId != null) return false;
            if (partitionInfo != null ? !partitionInfo.equals(request.partitionInfo) : request.partitionInfo != null)
                return false;
            if (partitionPrepareRoundId != null ? !partitionPrepareRoundId.equals(request.partitionPrepareRoundId) : request.partitionPrepareRoundId != null)
                return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = partitionPrepareRoundId != null ? partitionPrepareRoundId.hashCode() : 0;
            result = 31 * result + (partitionInfo != null ? partitionInfo.hashCode() : 0);
            result = 31 * result + (overlayId != null ? overlayId.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "Request{" +
                    "partitionPrepareRoundId=" + partitionPrepareRoundId +
                    ", partitionInfo=" + partitionInfo +
                    ", overlayId=" + overlayId +
                    '}';
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


        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Response)) return false;

            Response response = (Response) o;

            if (partitionPrepareRoundId != null ? !partitionPrepareRoundId.equals(response.partitionPrepareRoundId) : response.partitionPrepareRoundId != null)
                return false;
            if (partitionRequestId != null ? !partitionRequestId.equals(response.partitionRequestId) : response.partitionRequestId != null)
                return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = partitionPrepareRoundId != null ? partitionPrepareRoundId.hashCode() : 0;
            result = 31 * result + (partitionRequestId != null ? partitionRequestId.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "Response{" +
                    "partitionPrepareRoundId=" + partitionPrepareRoundId +
                    ", partitionRequestId=" + partitionRequestId +
                    '}';
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
