package se.sics.ms.serializer;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.ms.data.EntryExchange;
import se.sics.ms.helper.SerializerDecoderHelper;
import se.sics.ms.helper.SerializerEncoderHelper;
import se.sics.ms.types.ApplicationEntry;

import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;

/**
 * Serializers for the entry exchange request / response protocol
 * as part of entry pull mechanism.
 *
 * Created by babbar on 2015-06-21.
 */
public class EntryExchangeSerializer {



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

            EntryExchange.Request  request = (EntryExchange.Request)o;
            Serializers.lookupSerializer(UUID.class).toBinary(request.getExchangeRoundId(), buf);

            Collection<ApplicationEntry.ApplicationEntryId> entryIds = request.getEntryIds();
            Serializer serializer = Serializers.lookupSerializer(ApplicationEntry.ApplicationEntryId.class);

            SerializerEncoderHelper.collectionToBuff(entryIds, serializer, buf);
        }

        @Override
        public Object fromBinary(ByteBuf buf, Optional<Object> hint) {


            UUID exchangeRound = (UUID)Serializers.lookupSerializer(UUID.class).fromBinary(buf, hint);
            Collection<ApplicationEntry.ApplicationEntryId> entryIds = new ArrayList<ApplicationEntry.ApplicationEntryId>();
            Serializer serializer = Serializers.lookupSerializer(ApplicationEntry.ApplicationEntryId.class);

            SerializerDecoderHelper.readCollectionFromBuff(entryIds, serializer, buf, hint);
            return new EntryExchange.Request(exchangeRound, entryIds);
        }
    }


    public static class Response implements Serializer{

        private int id;

        public Response(int id){
            this.id  = id;
        }

        @Override
        public int identifier() {
            return this.id;
        }

        @Override
        public void toBinary(Object o, ByteBuf buf) {

            EntryExchange.Response response = (EntryExchange.Response)o;
            Serializers.lookupSerializer(UUID.class).toBinary(response.getEntryExchangeRound(), buf);

            Serializer serializer = Serializers.lookupSerializer(ApplicationEntry.class);
            Collection<ApplicationEntry> entries = response.getApplicationEntries();
            SerializerEncoderHelper.collectionToBuff(entries, serializer, buf);

            buf.writeInt(response.getOverlayId());

        }

        @Override
        public Object fromBinary(ByteBuf buf, Optional<Object> hint) {

            UUID exchangeRound = (UUID)Serializers.lookupSerializer(UUID.class).fromBinary(buf, hint);

            Collection<ApplicationEntry> entries = new ArrayList<ApplicationEntry>();
            Serializer serializer = Serializers.lookupSerializer(ApplicationEntry.class);
            SerializerDecoderHelper.readCollectionFromBuff(entries, serializer, buf, hint);

            int overlayId = buf.readInt();
            return new EntryExchange.Response(exchangeRound, entries, overlayId);
        }
    }

}
