package se.sics.ms.data;

import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.ms.types.EpochUpdatePacket;
import se.sics.ms.types.OverlayId;

import java.util.UUID;

/**
 *  
 * Created by babbarshaer on 2015-05-19.
 */
public class ShardingPrepare {
    
    
    public static class Request {
        
        private UUID shardRoundId;
        private EpochUpdatePacket epochUpdatePacket;
        private OverlayId overlayId;
        
        public Request( UUID shardRoundId, EpochUpdatePacket epochUpdatePacket, OverlayId overlayId ){
            
            this.shardRoundId = shardRoundId;
            this.epochUpdatePacket = epochUpdatePacket;
            this.overlayId = overlayId;
            
        }

        @Override
        public String toString() {
            return "Request{" +
                    "shardRoundId=" + shardRoundId +
                    ", shardInfo=" + epochUpdatePacket +
                    ", overlayId=" + overlayId +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Request request = (Request) o;

            if (overlayId != null ? !overlayId.equals(request.overlayId) : request.overlayId != null) return false;
            if (shardRoundId != null ? !shardRoundId.equals(request.shardRoundId) : request.shardRoundId != null)
                return false;
            if (epochUpdatePacket != null ? !epochUpdatePacket.equals(request.epochUpdatePacket) : request.epochUpdatePacket != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = shardRoundId != null ? shardRoundId.hashCode() : 0;
            result = 31 * result + (epochUpdatePacket != null ? epochUpdatePacket.hashCode() : 0);
            result = 31 * result + (overlayId != null ? overlayId.hashCode() : 0);
            return result;
        }

        public UUID getShardRoundId() {
            return shardRoundId;
        }

        public EpochUpdatePacket getEpochUpdatePacket() {
            return epochUpdatePacket;
        }

        public OverlayId getOverlayId() {
            return overlayId;
        }
    }
    
    
    public static class Response {
        
        private UUID shardRoundId;
        
        public Response(UUID shardRoundId){
            this.shardRoundId = shardRoundId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Response response = (Response) o;

            if (shardRoundId != null ? !shardRoundId.equals(response.shardRoundId) : response.shardRoundId != null)
                return false;

            return true;
        }

        @Override
        public int hashCode() {
            return shardRoundId != null ? shardRoundId.hashCode() : 0;
        }

        public UUID getShardRoundId() {
            return shardRoundId;
        }
        
        
    }
    

    public static class Timeout extends se.sics.kompics.timer.Timeout{

        public Timeout(ScheduleTimeout request) {
            super(request);
        }
    }
    
}
