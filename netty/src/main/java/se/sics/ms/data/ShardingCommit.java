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

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Request request = (Request) o;

            if (shardRoundId != null ? !shardRoundId.equals(request.shardRoundId) : request.shardRoundId != null)
                return false;

            return true;
        }

        @Override
        public int hashCode() {
            return shardRoundId != null ? shardRoundId.hashCode() : 0;
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
    }

}
