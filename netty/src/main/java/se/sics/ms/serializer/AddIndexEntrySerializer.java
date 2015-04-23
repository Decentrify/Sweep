package se.sics.ms.serializer;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.ms.data.AddIndexEntry;
import se.sics.ms.types.IndexEntry;

import java.util.UUID;

/**
 * Serializer for the add index entry protocol.
 *
 * Created by babbar on 2015-04-21.
 */
public class AddIndexEntrySerializer {


    public static class Request implements Serializer {

        private int id;

        public Request(int id){
            this.id = id;
        }

        @Override
        public int identifier() {
            return this.id;
        }

        @Override
        public void toBinary(Object o, ByteBuf byteBuf) {
            AddIndexEntry.Request request = (AddIndexEntry.Request)o;
            Serializers.lookupSerializer(UUID.class).toBinary(request.getEntryAdditionRound(), byteBuf);
            Serializers.lookupSerializer(IndexEntry.class).toBinary(request.getEntry(), byteBuf);
        }

        @Override
        public Object fromBinary(ByteBuf byteBuf, Optional<Object> optional) {

            UUID roundId = (UUID) Serializers.lookupSerializer(UUID.class).fromBinary(byteBuf, optional);
            IndexEntry indexEntry = (IndexEntry) Serializers.lookupSerializer(IndexEntry.class).fromBinary(byteBuf, optional);

            return new AddIndexEntry.Request(roundId, indexEntry);
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
        public void toBinary(Object o, ByteBuf byteBuf) {

            AddIndexEntry.Response response = (AddIndexEntry.Response)o;
            Serializers.lookupSerializer(UUID.class).toBinary(response.getEntryAdditionRound(), byteBuf);
        }

        @Override
        public Object fromBinary(ByteBuf byteBuf, Optional<Object> optional) {

            UUID roundId = (UUID) Serializers.lookupSerializer(UUID.class).fromBinary(byteBuf, optional);
            return new AddIndexEntry.Response(roundId);
        }
    }


}
