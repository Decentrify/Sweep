package se.sics.ms.serializer;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.ms.data.ApplicationEntryAddPrepare;
import se.sics.ms.data.LandingEntryAddPrepare;
import se.sics.ms.helper.SerializerDecoderHelper;
import se.sics.ms.helper.SerializerEncoderHelper;
import se.sics.ms.types.ApplicationEntry;
import se.sics.ms.types.LeaderUnit;

import java.util.UUID;

/**
 * Serializer for the Entry Addition Message for the 
 * Landing Entry.
 *  
 * Created by babbarshaer on 2015-06-23.
 */
public class ApplicationEntryAddSerializer {
    
    
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

            ApplicationEntryAddPrepare.Request request = (ApplicationEntryAddPrepare.Request) o;
            Serializers.lookupSerializer(UUID.class).toBinary(request.getEntryAdditionRound(), buf);
            Serializers.lookupSerializer(ApplicationEntry.class).toBinary(request.getApplicationEntry(), buf);

        }
        
        @Override
        public Object fromBinary(ByteBuf buf, Optional<Object> hint) {

            UUID entryAdditionRound = (UUID)Serializers.lookupSerializer(UUID.class).fromBinary(buf, hint);
            ApplicationEntry entry = (ApplicationEntry)Serializers.lookupSerializer(ApplicationEntry.class).fromBinary(buf, hint);

            return new ApplicationEntryAddPrepare.Request(entryAdditionRound, entry);
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

            ApplicationEntryAddPrepare.Response response = (ApplicationEntryAddPrepare.Response)o;
            Serializers.lookupSerializer(UUID.class).toBinary(response.getEntryAdditionRound(), buf);
            Serializers.lookupSerializer(ApplicationEntry.ApplicationEntryId.class).toBinary(response.getEntryId(), buf);
        }

        @Override
        public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
            
            UUID entryAdditionRound = (UUID)Serializers.lookupSerializer(UUID.class).fromBinary(buf, hint);
            ApplicationEntry.ApplicationEntryId entryId = (ApplicationEntry.ApplicationEntryId)Serializers.lookupSerializer(ApplicationEntry.ApplicationEntryId.class).fromBinary(buf, hint);
            
            return new ApplicationEntryAddPrepare.Response(entryAdditionRound, entryId);
        }
    }
    
    
}
