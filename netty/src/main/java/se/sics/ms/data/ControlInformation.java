package se.sics.ms.data;

import se.sics.ms.types.OverlayId;

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
    }

    public static class Response{
        
        private byte[] byteInfo;
        
        public Response(byte[] byteInfo){
            this.byteInfo = byteInfo;
        }

        public byte[] getByteInfo() {
            return byteInfo;
        }
    }
    
}
