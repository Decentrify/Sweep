package se.sics.ms.data;

import java.util.UUID;

/**
 * Container for information exchanged during 
 * the Epoch Commit Protocol.
 *
 * Created by babbarshaer on 2015-05-20.
 */
public class EpochAddCommit {
    
    
    public static class Request {
        
        public UUID epochAdditionRound;
        public Request(UUID epochAdditionRound){
            this.epochAdditionRound = epochAdditionRound;
        }
    }
    
    public static class Response {
        
        public UUID epochAdditionRound;

        public Response(UUID epochAdditionRound) {
            this.epochAdditionRound = epochAdditionRound;
        }
        
    }
    
    
}
