package se.sics.ms.data;

import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.kompics.timer.Timeout;
import se.sics.ms.types.EpochContainer;

import java.util.UUID;

/**
 * Main container class for the information exchange
 * between the nodes as part of epoch addition protocol.
 *  
 * @author babbarshaer 
 */
public class EpochAddPrepare {
    
    
    public static class Request {
        
        private UUID epochRoundID;
        private EpochContainer previousEpochUpdate;
        private EpochContainer currentEpochUpdate;

        public Request(UUID epochRoundID, EpochContainer previousEpochUpdate, EpochContainer currentEpochUpdate) {
            this.epochRoundID = epochRoundID;
            this.previousEpochUpdate = previousEpochUpdate;
            this.currentEpochUpdate = currentEpochUpdate;
        }

        public UUID getEpochRoundID() {
            return epochRoundID;
        }

        public EpochContainer getPreviousEpochUpdate() {
            return previousEpochUpdate;
        }

        public EpochContainer getCurrentEpochUpdate() {
            return currentEpochUpdate;
        }
    }
    
    
    public static class Response {
        
        private UUID epochAddRoundId;
        
        
        public Response(UUID epochAddRoundId){
            this.epochAddRoundId = epochAddRoundId;
        }

        public UUID getEpochAddRoundId() {
            return epochAddRoundId;
        }

    }
    
    
    
    
    public static class Timeout extends se.sics.kompics.timer.Timeout{

        public Timeout(ScheduleTimeout request) {
            super(request);
        }
    }
    
    
}
