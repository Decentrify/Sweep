package se.sics.ms.serializer;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.ms.data.EntryHashExchange;
import se.sics.ms.helper.SerializerDecoderHelper;
import se.sics.ms.helper.SerializerEncoderHelper;
import se.sics.ms.types.ApplicationEntry;
import se.sics.ms.types.EntryHash;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

/**
 * Serializer for the entry hash exchange in the
 * system.
 *
 * Created by babbar on 2015-06-21.
 */
public class EntryHashExchangeSerializer {


    public static class Request implements Serializer{

        private int id;

        public Request(int id){
            this.id = id;
        }

        @Override
        public int identifier() {
            return 0;
        }

        @Override
        public void toBinary(Object o, ByteBuf buf) {
            EntryHashExchange.Request request = (EntryHashExchange.Request)o;
            Serializers.lookupSerializer(UUID.class).toBinary(request.getExchangeRoundId(), buf);
            Serializers.lookupSerializer(ApplicationEntry.ApplicationEntryId.class).toBinary(request.getLowestMissingIndexEntry(), buf);
        }

        @Override
        public Object fromBinary(ByteBuf buf, Optional<Object> hint) {


            UUID exchangeRoundId = (UUID)Serializers.lookupSerializer(UUID.class).fromBinary(buf, hint);
            ApplicationEntry.ApplicationEntryId lowestMissingId = (ApplicationEntry.ApplicationEntryId)Serializers.lookupSerializer(ApplicationEntry.ApplicationEntryId.class).fromBinary(buf, hint);

            return new EntryHashExchange.Request(exchangeRoundId, lowestMissingId);
        }
    }

    public static class Response implements Serializer {

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

            EntryHashExchange.Response response = (EntryHashExchange.Response)o;
            Serializers.lookupSerializer(UUID.class).toBinary(response.getExchangeRoundId(), buf);

            Serializer serializer = Serializers.lookupSerializer(EntryHash.class);
            Collection<EntryHash> entryHashCollection = response.getEntryHashes();
            SerializerEncoderHelper.collectionToBuff(entryHashCollection, serializer, buf);
        }

        @Override
        public Object fromBinary(ByteBuf buf, Optional<Object> hint) {

            UUID exchangeRound = (UUID) Serializers.lookupSerializer(UUID.class).fromBinary(buf, hint);

            List<EntryHash> entryHashCollection = new ArrayList<EntryHash>();
            Serializer entryHashSerializer = Serializers.lookupSerializer(EntryHash.class);
            SerializerDecoderHelper.readCollectionFromBuff(entryHashCollection, entryHashSerializer, buf, hint);

            return new EntryHashExchange.Response(exchangeRound, entryHashCollection);
        }
    }


}
