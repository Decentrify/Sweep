package se.sics.ms.gradient.events;

import se.sics.kompics.KompicsEvent;

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
        
        public Request(long epochId, int leaderId){
            
            this.epochId = epochId;
            this.leaderId = leaderId;
        }


        public long getEpochId() {
            return epochId;
        }

        public int getLeaderId() {
            return leaderId;
        }
    }
    
    public static class Response implements KompicsEvent {

        private long epochId;
        private int leaderId;
        private boolean result;
        
        public Response(long epochId, int leaderId, boolean result){
            
            this.leaderId = leaderId;
            this.epochId = epochId;
            this.result = result;
        }


        public long getEpochId() {
            return epochId;
        }

        public int getLeaderId() {
            return leaderId;
        }

        public boolean isResult() {
            return result;
        }
    }
}
