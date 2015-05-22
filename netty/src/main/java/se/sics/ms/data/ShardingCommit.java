package se.sics.ms.data;

import java.util.UUID;

/**
 *  
 * Created by babbarshaer on 2015-05-19.
 */
public class ShardingCommit {
    
    
    public static class Request {
        
        private UUID shardRoundId;
        
        public Request(UUID shardRoundId){
            this.shardRoundId = shardRoundId;
        }
        
        public UUID getShardRoundId(){
            return this.shardRoundId;
        }
    }
    
    
    public static class Response {
        
        private UUID shardRoundId;
        
        public Response(UUID shardRoundId){
            this.shardRoundId = shardRoundId;
        }

        public UUID getShardRoundId() {
            return shardRoundId;
        }
    }

}
