package se.sics.ms.partitionaware.api.events;

import se.sics.kompics.KompicsEvent;

import java.util.UUID;

/**
 * Wrapper for the message exchange between the application and the 
 * PAG in order to determine if particular LU lies in the evolution history
 * of the application.<br/>
 * 
 * <i>Leader Unit Check</i>
 *
 * <br/> 
 * Created by babbarshaer on 2015-06-06.
 */
public class LUCheck {
    
    
    public static class Request implements KompicsEvent {
        
        private long epochId;
        private int leaderId;
        private long maxEntries;
        private UUID requestId;
        
        public Request( UUID requestId, long epochId, int leaderId, long maxEntries ){
            
            this.requestId = requestId;
            this.epochId = epochId;
            this.leaderId = leaderId;
            this.maxEntries = maxEntries;
            
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Request request = (Request) o;

            if (epochId != request.epochId) return false;
            if (leaderId != request.leaderId) return false;
            if (maxEntries != request.maxEntries) return false;
            if (requestId != null ? !requestId.equals(request.requestId) : request.requestId != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = (int) (epochId ^ (epochId >>> 32));
            result = 31 * result + leaderId;
            result = 31 * result + (int) (maxEntries ^ (maxEntries >>> 32));
            result = 31 * result + (requestId != null ? requestId.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "Request{" +
                    "epochId=" + epochId +
                    ", leaderId=" + leaderId +
                    ", maxEntries=" + maxEntries +
                    ", requestId=" + requestId +
                    '}';
        }

        public long getMaxEntries() {
            return maxEntries;
        }

        public UUID getRequestId() {
            return requestId;
        }

        public long getEpochId() {
            return epochId;
        }

        public int getLeaderId() {
            return leaderId;
        }
    }
    
    public static class Response implements KompicsEvent {

        private UUID requestId;
        private boolean result;


        public Response(UUID requestId, boolean result) {
            this.requestId = requestId;
            this.result = result;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Response response = (Response) o;

            if (result != response.result) return false;
            if (requestId != null ? !requestId.equals(response.requestId) : response.requestId != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result1 = requestId != null ? requestId.hashCode() : 0;
            result1 = 31 * result1 + (result ? 1 : 0);
            return result1;
        }


        @Override
        public String toString() {
            return "Response{" +
                    "requestId=" + requestId +
                    ", result=" + result +
                    '}';
        }

        public UUID getRequestId() {
            return requestId;
        }

        public boolean isResult() {
            return result;
        }
    }
        
}
