package se.sics.ms.data;

import se.sics.ms.types.OverlayId;

import java.util.Arrays;
import java.util.UUID;

/**
 * Container class for the control information request to the 
 * different components in the peer.
 * 
 * Created by babbarshaer on 2015-04-19.
 */
public class ControlInformation {
    
    
    public static class Request{
        
        private UUID requestId;
        private OverlayId overlayId;
                
        public Request(UUID requestId, OverlayId overlayId){
            this.requestId = requestId;
            this.overlayId= overlayId;
        }

        public UUID getRequestId() {
            return requestId;
        }

        public OverlayId getOverlayId() {
            return overlayId;
        }


        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Request)) return false;

            Request request = (Request) o;

            if (overlayId != null ? !overlayId.equals(request.overlayId) : request.overlayId != null) return false;
            if (requestId != null ? !requestId.equals(request.requestId) : request.requestId != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = requestId != null ? requestId.hashCode() : 0;
            result = 31 * result + (overlayId != null ? overlayId.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "Request{" +
                    "requestId=" + requestId +
                    ", overlayId=" + overlayId +
                    '}';
        }
    }

    public static class Response{
        
        private byte[] byteInfo;
        private UUID roundId;
        
        public Response(UUID roundId, byte[] byteInfo){
            this.roundId = roundId;
            this.byteInfo = byteInfo;
        }

        public byte[] getByteInfo() {
            return byteInfo;
        }
        
        public UUID getRoundId(){
            return this.roundId;
        }

        @Override
        public boolean equals(Object o) {

            if (this == o) return true;
            if (!(o instanceof Response)) return false;

            Response response = (Response) o;

            if (!Arrays.equals(byteInfo, response.byteInfo)) return false;
            if (roundId != null ? !roundId.equals(response.roundId) : response.roundId != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = byteInfo != null ? Arrays.hashCode(byteInfo) : 0;
            result = 31 * result + (roundId != null ? roundId.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "Response{" +
                    "byteInfo=" + Arrays.toString(byteInfo) +
                    ", roundId=" + roundId +
                    '}';
        }
    }
    
}
