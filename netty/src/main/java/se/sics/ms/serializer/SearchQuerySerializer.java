package se.sics.ms.serializer;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.ms.data.SearchQuery;
import se.sics.ms.types.IndexEntry;
import se.sics.ms.types.SearchPattern;
import se.sics.ms.util.IdScorePair;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Serializer for the query phase exchanges in the search 
 * protocol.
 *  
 * Created by babbarshaer on 2015-07-20.
 */
public class SearchQuerySerializer {
    
    
    public static class Request implements Serializer{

        private int id;
        
        public Request(int id){
            this.id = id;
        }

        @Override
        public int identifier() {
            return this.id;
        }

        @Override
        public void toBinary(Object o, ByteBuf buf) {
            SearchQuery.Request request = (SearchQuery.Request)o;
            
            Serializers.lookupSerializer(UUID.class).toBinary(request.getRequestId(), buf);
            buf.writeInt(request.getPartitionId());
            Serializers.lookupSerializer(SearchPattern.class).toBinary(request.getRequestId(), buf);
        }

        @Override
        public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
            
            UUID requestId = (UUID) Serializers.lookupSerializer(UUID.class).fromBinary(buf, hint);
            int partitionId = buf.readInt();
            SearchPattern pattern = (SearchPattern)Serializers.lookupSerializer(SearchPattern.class).fromBinary(buf, hint);
                    
            return new SearchQuery.Request(requestId, partitionId, pattern);
        }
    }
    
    
    
    public static class Response implements Serializer{

        private int id;

        public Response(int id){
            this.id = id;
        }
        
        
        @Override
        public int identifier() {
            return this.id;
        }

        @Override
        public void toBinary(Object o, ByteBuf buf) {
            
            SearchQuery.Response response = (SearchQuery.Response)o;
            
            Serializers.lookupSerializer(UUID.class).toBinary(response.getSearchTimeoutId(), buf);
            buf.writeInt(response.getPartitionId());
            
            Serializer serializer = Serializers.lookupSerializer(IdScorePair.class);
            List<IdScorePair> collection = response.getIdScorePairCollection();
            if(collection == null){
                buf.writeInt(0);
                collection = new ArrayList<IdScorePair>();
            }
            else{
                buf.writeInt(collection.size());
            }

            for(IdScorePair scorePair : collection){
                serializer.toBinary(scorePair, buf);
            }



        }

        @Override
        public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
            
            UUID searchRoundId = (UUID)Serializers.lookupSerializer(UUID.class).fromBinary(buf, hint);
            int partitionId = buf.readInt();
            
            Serializer serializer = Serializers.lookupSerializer(IdScorePair.class);
            int size = buf.readInt();
            List<IdScorePair> collection = new ArrayList<IdScorePair>();
            
            while(size > 0){
                
                IdScorePair pair = (IdScorePair) serializer.fromBinary(buf, hint);
                collection.add(pair);
                size --;
            }
            
            return new SearchQuery.Response(searchRoundId, partitionId, collection);
        }
    }
    
}
