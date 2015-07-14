package se.sics.ms.data;

import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.ms.types.LeaderUnit;

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
        private LeaderUnit previousEpochUpdate;
        private LeaderUnit currentEpochUpdate;

        public Request(UUID epochRoundID, LeaderUnit previousEpochUpdate, LeaderUnit currentEpochUpdate) {
            this.epochRoundID = epochRoundID;
            this.previousEpochUpdate = previousEpochUpdate;
            this.currentEpochUpdate = currentEpochUpdate;
        }

        public UUID getEpochRoundID() {
            return epochRoundID;
        }

        public LeaderUnit getPreviousEpochUpdate() {
            return previousEpochUpdate;
        }

        public LeaderUnit getCurrentEpochUpdate() {
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
