package se.sics.ms.serializer;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.ms.data.ReplicationPrepareCommit;
import se.sics.ms.types.IndexEntry;

import java.util.UUID;

/**
 * Wrapper for the serializers for the container of the information
 * exchanged as part of entry addition promise phase of the entry addition protocol.
 *
 * Created by babbar on 2015-04-21.
 */
public class ReplicationPrepareCommitSerializer {


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
            ReplicationPrepareCommit.Request request = (ReplicationPrepareCommit.Request)o;
            Serializers.lookupSerializer(UUID.class).toBinary(request.getIndexAdditionRoundId(), byteBuf);
            Serializers.lookupSerializer(IndexEntry.class).toBinary(request.getEntry(), byteBuf);

        }

        @Override
        public Object fromBinary(ByteBuf byteBuf, Optional<Object> optional) {

            UUID preparePhaseId = (UUID) Serializers.lookupSerializer(UUID.class).fromBinary(byteBuf, optional);
            IndexEntry indexEntry = (IndexEntry) Serializers.lookupSerializer(IndexEntry.class).fromBinary(byteBuf, optional);

            return new ReplicationPrepareCommit.Request(indexEntry, preparePhaseId);
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

            ReplicationPrepareCommit.Response response = (ReplicationPrepareCommit.Response)o;
            Serializers.lookupSerializer(UUID.class).toBinary(response.getIndexAdditionRoundId(), byteBuf);
            byteBuf.writeLong(response.getEntryId());

        }

        @Override
        public Object fromBinary(ByteBuf byteBuf, Optional<Object> optional) {

            UUID preparePhaseId = (UUID) Serializers.lookupSerializer(UUID.class).fromBinary(byteBuf, optional);
            long entryId = byteBuf.readLong();

            return new ReplicationPrepareCommit.Response(preparePhaseId, entryId);
        }
    }


}
