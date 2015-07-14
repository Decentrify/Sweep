package se.sics.ms.serializer;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.ms.data.LeaderPullEntry;
import se.sics.ms.helper.SerializerDecoderHelper;
import se.sics.ms.helper.SerializerEncoderHelper;
import se.sics.ms.types.ApplicationEntry;

import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;

/**
 * Serializer for the message exchange between
 * nodes in the system as part of the Entry Exchange.
 *
 * Created by babbar on 2015-06-21.
 */
public class LeaderPullEntrySerializer {


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
        public void toBinary(Object o, ByteBuf buf) {

            LeaderPullEntry.Request request = (LeaderPullEntry.Request)o;
            Serializers.lookupSerializer(UUID.class).toBinary(request.getDirectPullRound(), buf);
            Serializers.lookupSerializer(ApplicationEntry.ApplicationEntryId.class).toBinary(request.getLowestMissingEntryId(), buf);

        }

        @Override
        public Object fromBinary(ByteBuf buf, Optional<Object> hint) {


            UUID pullRound = (UUID) Serializers.lookupSerializer(UUID.class).fromBinary(buf, hint);
            ApplicationEntry.ApplicationEntryId missingId = (ApplicationEntry.ApplicationEntryId) Serializers.lookupSerializer(ApplicationEntry.ApplicationEntryId.class).fromBinary(buf, hint);

            return new LeaderPullEntry.Request(pullRound, missingId);
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

            LeaderPullEntry.Response response = (LeaderPullEntry.Response)o;

            // Write UUID Serializer.
            Serializers.lookupSerializer(UUID.class).toBinary(response.getDirectPullRound(), buf);

            // Write Application Entries in the system.
            Collection<ApplicationEntry> entries = response.getMissingEntries();
            Serializer entrySerializer = Serializers.lookupSerializer(ApplicationEntry.class);
            SerializerEncoderHelper.collectionToBuff(entries, entrySerializer, buf);

            // Write Overlay Id.
            buf.writeInt(response.getOverlayId());

        }

        @Override
        public Object fromBinary(ByteBuf buf, Optional<Object> hint) {


            UUID directPullRound = (UUID) Serializers.lookupSerializer(UUID.class).fromBinary(buf, hint);

            Collection<ApplicationEntry> entries = new ArrayList<ApplicationEntry>();
            Serializer entrySerializer = Serializers.lookupSerializer(ApplicationEntry.class);
            SerializerDecoderHelper.readCollectionFromBuff(entries, entrySerializer, buf, hint);
            int overlayId = buf.readInt();

            return new LeaderPullEntry.Response(directPullRound, entries, overlayId);
        }
    }




}
