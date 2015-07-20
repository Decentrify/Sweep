package se.sics.ms.serializer;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.ms.data.SearchFetch;
import se.sics.ms.types.ApplicationEntry;

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
            
            Collection<ApplicationEntry.ApplicationEntryId> entryIds = request.getEntryIds();
            if(entryIds == null){
                buf.writeInt(0);
                
            }
            else {
                
                Serializer serializer = Serializers.lookupSerializer(ApplicationEntry.ApplicationEntryId.class);
                buf.writeInt(entryIds.size());
                for(ApplicationEntry.ApplicationEntryId entryId : entryIds){
                    serializer.toBinary(entryId, buf);
                }
            }
            
        }

        @Override
        public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
            
            
            UUID requestId = (UUID)Serializers.lookupSerializer(UUID.class).fromBinary(buf, hint);
            List<ApplicationEntry.ApplicationEntryId> entryIdList = new ArrayList<ApplicationEntry.ApplicationEntryId>();
            
            int size = buf.readInt();
            Serializer serializer = Serializers.lookupSerializer(ApplicationEntry.ApplicationEntryId.class);
            while(size > 0){
                
                ApplicationEntry.ApplicationEntryId entryId = (ApplicationEntry.ApplicationEntryId) serializer.fromBinary(buf, hint);
                entryIdList.add(entryId);
                size --;
            }
            
            return new SearchFetch.Request(requestId, entryIdList);
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
            
            Serializer serializer = Serializers.lookupSerializer(ApplicationEntry.class);
            Collection<ApplicationEntry> collection = response.getApplicationEntries();
            
            if(collection == null){
                buf.writeInt(0);
            }
            else{
                buf.writeInt(collection.size());
                for(ApplicationEntry entry : collection){
                    serializer.toBinary(entry, buf);
                }
            }
        }

        @Override
        public Object fromBinary(ByteBuf buf, Optional<Object> hint) {

            UUID searchRoundId = (UUID) Serializers.lookupSerializer(UUID.class).fromBinary(buf, hint);
            Serializer serializer = Serializers.lookupSerializer(ApplicationEntry.class);
            List<ApplicationEntry> list = new ArrayList<ApplicationEntry>();
            
            int size = buf.readInt();
            while(size > 0){
                
                ApplicationEntry entry = (ApplicationEntry) serializer.fromBinary(buf, hint);
                list.add(entry);
                size --;
            }
            
            return new SearchFetch.Response(searchRoundId, list);
        }
    }
    
    
}
