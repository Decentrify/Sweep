package se.sics.ms.data;

import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.kompics.timer.Timeout;
import se.sics.ms.types.OverlayId;
import se.sics.ms.util.ShardInfo;

import java.util.UUID;

/**
 *  
 * Created by babbarshaer on 2015-05-19.
 */
public class ShardingPrepare {
    
    
    public static class Request {
        
        private UUID prepareRoundId;
        private ShardInfo shardInfo;
        private OverlayId overlayId;
        
        public Request( UUID prepareRoundId, ShardInfo shardInfo, OverlayId overlayId ){
            
            this.prepareRoundId = prepareRoundId;
            this.shardInfo = shardInfo;
            this.overlayId = overlayId;
            
        }

        @Override
        public String toString() {
            return "Request{" +
                    "prepareRoundId=" + prepareRoundId +
                    ", shardInfo=" + shardInfo +
                    ", overlayId=" + overlayId +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Request request = (Request) o;

            if (overlayId != null ? !overlayId.equals(request.overlayId) : request.overlayId != null) return false;
            if (prepareRoundId != null ? !prepareRoundId.equals(request.prepareRoundId) : request.prepareRoundId != null)
                return false;
            if (shardInfo != null ? !shardInfo.equals(request.shardInfo) : request.shardInfo != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = prepareRoundId != null ? prepareRoundId.hashCode() : 0;
            result = 31 * result + (shardInfo != null ? shardInfo.hashCode() : 0);
            result = 31 * result + (overlayId != null ? overlayId.hashCode() : 0);
            return result;
        }

        public UUID getPrepareRoundId() {
            return prepareRoundId;
        }

        public ShardInfo getShardInfo() {
            return shardInfo;
        }

        public OverlayId getOverlayId() {
            return overlayId;
        }
    }
    
    
    
    public static class Response {
        
        
    }
    

    public static class Timeout extends se.sics.kompics.timer.Timeout{

        public Timeout(ScheduleTimeout request) {
            super(request);
        }
    }
    
}
