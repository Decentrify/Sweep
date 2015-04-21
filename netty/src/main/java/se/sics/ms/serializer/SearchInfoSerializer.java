package se.sics.ms.serializer;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.ms.data.SearchInfo;
import se.sics.ms.types.IndexEntry;
import se.sics.ms.types.SearchPattern;

import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;

/**
 * Wrapper for the serializers used for encoding / decoding of the messages exchanged as
 * part of the search request / response protocol.
 *
 * Created by babbar on 2015-04-21.
 */
public class SearchInfoSerializer {


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

            SearchInfo.Request request = (SearchInfo.Request)o;
            Serializers.lookupSerializer(UUID.class).toBinary(request.getRequestId(), byteBuf);
            Serializers.lookupSerializer(SearchPattern.class).toBinary(request.getPattern(), byteBuf);
            byteBuf.writeInt(request.getPartitionId());

        }

        @Override
        public Object fromBinary(ByteBuf byteBuf, Optional<Object> optional) {

            UUID requestId = (UUID)Serializers.lookupSerializer(UUID.class).fromBinary(byteBuf, optional);
            SearchPattern pattern = (SearchPattern)Serializers.lookupSerializer(SearchPattern.class).fromBinary(byteBuf, optional);
            int partitionId = byteBuf.readInt();

            return new SearchInfo.Request(requestId, partitionId, pattern);
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
            SearchInfo.Response response = (SearchInfo.Response)o;
            Serializers.lookupSerializer(UUID.class).toBinary(response.getSearchTimeoutId(), byteBuf);
            Serializer entrySerializer = Serializers.lookupSerializer(IndexEntry.class);

            Collection<IndexEntry> entryCollection = response.getResults();
            if(entryCollection == null){
                byteBuf.writeInt(0);
                entryCollection = new ArrayList<IndexEntry>();
            }
            else{
                byteBuf.writeInt(entryCollection.size());
            }

            for(IndexEntry entry : entryCollection){
                entrySerializer.toBinary(entry, byteBuf);
            }


            byteBuf.writeInt(response.getPartitionId());
            byteBuf.writeInt(response.getNumResponses());
            byteBuf.writeInt(response.getResponseNumber());

        }

        @Override
        public Object fromBinary(ByteBuf byteBuf, Optional<Object> optional) {

            UUID uuid = (UUID)Serializers.lookupSerializer(UUID.class).fromBinary(byteBuf, optional);
            Serializer entrySerializer = Serializers.lookupSerializer(IndexEntry.class);

            Collection<IndexEntry> entryCollection = new ArrayList<IndexEntry>();

            int len = byteBuf.readInt();
            while(len > 0){

                IndexEntry entry = (IndexEntry)entrySerializer.fromBinary(byteBuf, optional);
                entryCollection.add(entry);
                len --;
            }


            int partitionId = byteBuf.readInt();
            int responses = byteBuf.readInt();
            int responseNumber = byteBuf.readInt();

            return new SearchInfo.Response(uuid, entryCollection, partitionId, responses, responseNumber);
        }
    }



}
