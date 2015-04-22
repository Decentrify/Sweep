package se.sics.ms.serializer;


import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.ms.data.PartitionCommit;

import java.util.UUID;

/**
 * Container for the serializers for the Partition commit phase of partitioning protocol.
 * Created by babbar on 2015-04-21.
 */
public class PartitionCommitSerializer {


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

            PartitionCommit.Request request = (PartitionCommit.Request)o;
            Serializer uuidSerializer = Serializers.lookupSerializer(UUID.class);
            uuidSerializer.toBinary(request.getPartitionRequestId(), byteBuf);
            uuidSerializer.toBinary(request.getPartitionCommitTimeout(), byteBuf);

        }

        @Override
        public Object fromBinary(ByteBuf byteBuf, Optional<Object> optional) {

            Serializer uuidSerializer = Serializers.lookupSerializer(UUID.class);

            UUID partitionRequestId = (UUID) uuidSerializer.fromBinary(byteBuf, optional);
            UUID partitionCommitTimeout = (UUID) uuidSerializer.fromBinary(byteBuf, optional);

            return new PartitionCommit.Request(partitionCommitTimeout, partitionRequestId);
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

            PartitionCommit.Response response = (PartitionCommit.Response)o;
            Serializer uuidSerializer = Serializers.lookupSerializer(UUID.class);
            uuidSerializer.toBinary(response.getPartitionRequestId(), byteBuf);
            uuidSerializer.toBinary(response.getPartitionCommitTimeout(), byteBuf);

        }

        @Override
        public Object fromBinary(ByteBuf byteBuf, Optional<Object> optional) {

            Serializer uuidSerializer = Serializers.lookupSerializer(UUID.class);

            UUID partitionRequestId = (UUID) uuidSerializer.fromBinary(byteBuf, optional);
            UUID partitionCommitTimeout = (UUID) uuidSerializer.fromBinary(byteBuf, optional);

            return new PartitionCommit.Response(partitionCommitTimeout, partitionRequestId);
        }
    }

}
