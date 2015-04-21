package se.sics.ms.serializer;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.ms.data.PartitionPrepare;
import se.sics.ms.types.OverlayId;
import se.sics.ms.util.PartitionHelper;

import java.util.UUID;

/**
 * Container class for the partition prepare phase of the partitioning protocol.
 * Created by babbar on 2015-04-21.
 */
public class PartitionPrepareSerializer{


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
            PartitionPrepare.Request request = (PartitionPrepare.Request)o;
            Serializers.lookupSerializer(UUID.class).toBinary(request.getPartitionPrepareRoundId(), byteBuf);
            Serializers.lookupSerializer(PartitionHelper.PartitionInfo.class).toBinary(request.getPartitionInfo(), byteBuf);
            byteBuf.writeInt(request.getOverlayId().getId());
        }

        @Override
        public Object fromBinary(ByteBuf byteBuf, Optional<Object> optional) {

            UUID partitionPrepareRound = (UUID) Serializers.lookupSerializer(UUID.class).fromBinary(byteBuf, optional);
            PartitionHelper.PartitionInfo partitionInfo = (PartitionHelper.PartitionInfo)Serializers.lookupSerializer(PartitionHelper.PartitionInfo.class).fromBinary(byteBuf, optional);
            OverlayId overlayId = new OverlayId(byteBuf.readInt());

            return new PartitionPrepare.Request(partitionPrepareRound, partitionInfo, overlayId);
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
            PartitionPrepare.Response response = (PartitionPrepare.Response)o;
            Serializer uuidSerializer = Serializers.lookupSerializer(UUID.class);

            uuidSerializer.toBinary(response.getPartitionPrepareRoundId(), byteBuf);
            uuidSerializer.toBinary(response.getPartitionRequestId(), byteBuf);
        }

        @Override
        public Object fromBinary(ByteBuf byteBuf, Optional<Object> optional) {

            Serializer uuidSerializer = Serializers.lookupSerializer(UUID.class);
            UUID partitionPrepareRound = (UUID) uuidSerializer.fromBinary(byteBuf, optional);
            UUID partitionRequestId  = (UUID) uuidSerializer.fromBinary(byteBuf, optional);

            return new PartitionPrepare.Response(partitionPrepareRound, partitionRequestId);
        }
    }

}
