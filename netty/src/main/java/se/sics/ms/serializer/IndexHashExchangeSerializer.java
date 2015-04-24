package se.sics.ms.serializer;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.ms.data.IndexHashExchange;
import se.sics.ms.types.IndexHash;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

/**
 * Created by babbar on 2015-04-24.
 */
public class IndexHashExchangeSerializer {


    public static class Request implements Serializer{

        private final int id;

        public Request(int id){
            this.id = id;
        }

        @Override
        public int identifier() {
            return this.id;
        }

        @Override
        public void toBinary(Object o, ByteBuf byteBuf) {

            IndexHashExchange.Request request = (IndexHashExchange.Request)o;
            Serializers.lookupSerializer(UUID.class).toBinary(request.getExchangeRoundId(), byteBuf);
            byteBuf.writeLong(request.getLowestMissingIndexEntry());

            Long[] entries = request.getEntries();

            if(entries == null){
                byteBuf.writeInt(0);
                entries = new Long[0];
            }
            else{
                byteBuf.writeInt(entries.length);
            }

            for(long entry : entries){
                byteBuf.writeLong(entry);
            }
        }

        @Override
        public Object fromBinary(ByteBuf byteBuf, Optional<Object> optional) {

            UUID exchangeRoundId = (UUID) Serializers.lookupSerializer(UUID.class).fromBinary(byteBuf, optional);
            long lowestMissingEntry = byteBuf.readLong();

            int size = byteBuf.readInt();
            List<Long> entries = new ArrayList<Long>(size);

            while(size > 0){
                entries.add(byteBuf.readLong());
                size --;
            }

            Long[] entriesArray = (Long[]) entries.toArray();
            return new IndexHashExchange.Request(exchangeRoundId, lowestMissingEntry, entriesArray);
        }
    }





    public static class Response implements Serializer {


        private final int id;

        public Response(int id){
            this.id = id;
        }

        @Override
        public int identifier() {
            return this.id;
        }

        @Override
        public void toBinary(Object o, ByteBuf byteBuf) {

            IndexHashExchange.Response response = (IndexHashExchange.Response)o;
            Serializer indexHashS = Serializers.lookupSerializer(IndexHash.class);
            Collection<IndexHash> collection = response.getIndexHashes();

            if(collection == null){
                byteBuf.writeInt(0);
                collection = new ArrayList<IndexHash>();
            }
            else{
                byteBuf.writeInt(collection.size());
            }

            for(IndexHash hash : collection){
                indexHashS.toBinary(hash, byteBuf);
            }

            Serializers.lookupSerializer(UUID.class).toBinary(response.getExchangeRoundId(), byteBuf);
        }

        @Override
        public Object fromBinary(ByteBuf byteBuf, Optional<Object> optional) {

            Serializer indexHashS = Serializers.lookupSerializer(IndexHash.class);
            int size = byteBuf.readInt();

            List<IndexHash> indexHashCollection = new ArrayList<IndexHash>();
            while(size > 0){
                IndexHash value = (IndexHash)indexHashS.fromBinary(byteBuf, optional);
                indexHashCollection.add(value);
            }

            UUID exchangeRoundId = (UUID) Serializers.lookupSerializer(UUID.class).fromBinary(byteBuf, optional);
            return new IndexHashExchange.Response(exchangeRoundId, indexHashCollection);
        }
    }




}
