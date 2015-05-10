package se.sics.ms.serializer;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.ms.data.IndexExchange;
import se.sics.ms.types.Id;
import se.sics.ms.types.IndexEntry;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

/**
 * Main serializer for the index exchange protocol.
 *
 * Created by babbar on 2015-04-21.
 */
public class IndexExchangeSerializer {


    public static class Request implements Serializer{

        private final int id;

        public Request(int id) {
            this.id = id;
        }

        @Override
        public int identifier() {
            return this.id;
        }

        @Override
        public void toBinary(Object o, ByteBuf byteBuf) {

            IndexExchange.Request request = (IndexExchange.Request)o;
            Serializers.lookupSerializer(UUID.class).toBinary(request.getExchangeRoundId(), byteBuf);
            byteBuf.writeInt(request.getIds().size());
            for(Id id : request.getIds()){
                Serializers.lookupSerializer(Id.class).toBinary(id, byteBuf);
            }
            
            byteBuf.writeInt(request.getOverlayId());
        }

        @Override
        public Object fromBinary(ByteBuf byteBuf, Optional<Object> optional) {

            UUID roundId = (UUID)Serializers.lookupSerializer(UUID.class).fromBinary(byteBuf, optional);
            int idLen = byteBuf.readInt();

            List<Id> idList = new ArrayList<Id>();
            while(idLen > 0){

                Id id = (Id) Serializers.lookupSerializer(Id.class).fromBinary(byteBuf, optional);
                idList.add(id);
                idLen --;
            }
            
           int overlayId = byteBuf.readInt();
            return new IndexExchange.Request(roundId, idList, overlayId);
        }
    }



    public static class Response implements Serializer{


        private final int id;

        public Response(int id) {
            this.id = id;
        }

        @Override
        public int identifier() {
            return this.id;
        }

        @Override
        public void toBinary(Object o, ByteBuf byteBuf) {

            IndexExchange.Response response = (IndexExchange.Response)o;
            Serializers.lookupSerializer(UUID.class).toBinary(response.getExchangeRoundId(), byteBuf);
            Serializer entrySerializer = Serializers.lookupSerializer(IndexEntry.class);

            Collection<IndexEntry> entryCollection = response.getIndexEntries();
            byteBuf.writeInt(entryCollection.size());

            for(IndexEntry entry : entryCollection){
                entrySerializer.toBinary(entry, byteBuf);
            }

            byteBuf.writeInt(response.getNumResponses());
            byteBuf.writeInt(response.getResponseNumber());
            byteBuf.writeInt(response.getOverlayId());
        }

        @Override
        public Object fromBinary(ByteBuf byteBuf, Optional<Object> optional) {

            UUID roundId = (UUID) Serializers.lookupSerializer(UUID.class).fromBinary(byteBuf, optional);
            Serializer entrySerializer = Serializers.lookupSerializer(IndexEntry.class);

            int entryLen = byteBuf.readInt();
            Collection<IndexEntry> entryCollection = new ArrayList<IndexEntry>();
            while(entryLen > 0){

                IndexEntry entry = (IndexEntry) entrySerializer.fromBinary(byteBuf, optional);
                entryCollection.add(entry);
                entryLen --;
            }

            int numResponses = byteBuf.readInt();
            int responseNumber = byteBuf.readInt();
            int overlayId = byteBuf.readInt();

            return new IndexExchange.Response(roundId, entryCollection, numResponses, responseNumber,overlayId);
        }
    }


}
