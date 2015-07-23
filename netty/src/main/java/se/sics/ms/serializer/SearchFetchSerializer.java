package se.sics.ms.serializer;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.ms.data.SearchFetch;
import se.sics.ms.util.EntryScorePair;
import se.sics.ms.util.IdScorePair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

/**
 * Serializer for the fetch phase of the search protocol.
 * Created by babbarshaer on 2015-07-20.
 */
public class SearchFetchSerializer {
    
    
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

            SearchFetch.Request request = (SearchFetch.Request)o;
            Serializers.lookupSerializer(UUID.class).toBinary(request.getFetchRequestId(), buf);
            
            Collection<IdScorePair> entryIds = request.getEntryIds();
            if(entryIds == null){
                buf.writeInt(0);
                
            }
            else {
                
                Serializer serializer = Serializers.lookupSerializer(IdScorePair.class);
                buf.writeInt(entryIds.size());
                for(IdScorePair pair : entryIds){
                    serializer.toBinary(pair, buf);
                }
            }
            
        }

        @Override
        public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
            
            
            UUID requestId = (UUID)Serializers.lookupSerializer(UUID.class).fromBinary(buf, hint);
            List<IdScorePair> idScorePairList = new ArrayList<IdScorePair>();
            
            int size = buf.readInt();
            Serializer serializer = Serializers.lookupSerializer(IdScorePair.class);
            while(size > 0){
                
                IdScorePair idScorePair = (IdScorePair) serializer.fromBinary(buf, hint);
                idScorePairList.add(idScorePair);
                size --;
            }
            
            return new SearchFetch.Request(requestId, idScorePairList);
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
            
            SearchFetch.Response response = (SearchFetch.Response)o;
            Serializers.lookupSerializer(UUID.class).toBinary(response.getFetchRequestId(), buf);
            
            Serializer serializer = Serializers.lookupSerializer(EntryScorePair.class);
            Collection<EntryScorePair> collection = response.getEntryScorePairs();
            
            if(collection == null){
                buf.writeInt(0);
            }
            else{
                buf.writeInt(collection.size());
                for(EntryScorePair entry : collection){
                    serializer.toBinary(entry, buf);
                }
            }
        }

        @Override
        public Object fromBinary(ByteBuf buf, Optional<Object> hint) {

            UUID searchRoundId = (UUID) Serializers.lookupSerializer(UUID.class).fromBinary(buf, hint);
            Serializer serializer = Serializers.lookupSerializer(EntryScorePair.class);
            List<EntryScorePair> list = new ArrayList<EntryScorePair>();
            
            int size = buf.readInt();
            while(size > 0){
                
                EntryScorePair entry = (EntryScorePair) serializer.fromBinary(buf, hint);
                list.add(entry);
                size --;
            }
            
            return new SearchFetch.Response(searchRoundId, list);
        }
    }
    
    
}
