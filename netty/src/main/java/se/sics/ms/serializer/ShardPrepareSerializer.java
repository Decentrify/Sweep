package se.sics.ms.serializer;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.ms.data.ShardingPrepare;
import se.sics.ms.types.LeaderUnitUpdate;
import se.sics.ms.types.OverlayId;

import java.util.UUID;

/**
 * Serializer for the prepare phase message exchange of the
 * sharding protocol.
 *
 * Created by babbar on 2015-06-22.
 */
public class ShardPrepareSerializer {


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

            ShardingPrepare.Request request = (ShardingPrepare.Request)o;
            Serializers.lookupSerializer(UUID.class).toBinary(request.getShardRoundId(), buf);
            Serializers.lookupSerializer(LeaderUnitUpdate.class).toBinary(request.getEpochUpdatePacket(), buf);
            Serializers.lookupSerializer(OverlayId.class).toBinary(request.getOverlayId(), buf);

        }

        @Override
        public Object fromBinary(ByteBuf buf, Optional<Object> hint) {

            UUID shardRoundId = (UUID)Serializers.lookupSerializer(UUID.class).fromBinary(buf, hint);
            LeaderUnitUpdate unitUpdate = (LeaderUnitUpdate)Serializers.lookupSerializer(LeaderUnitUpdate.class).fromBinary(buf, hint);
            OverlayId overlayId = (OverlayId)Serializers.lookupSerializer(OverlayId.class).fromBinary(buf, hint);

            return new ShardingPrepare.Request(shardRoundId, unitUpdate, overlayId);
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

            ShardingPrepare.Response response = (ShardingPrepare.Response)o;
            Serializers.lookupSerializer(UUID.class).toBinary(response.getShardRoundId(), buf);
        }

        @Override
        public Object fromBinary(ByteBuf buf, Optional<Object> hint) {

            UUID shardRoundId = (UUID)Serializers.lookupSerializer(UUID.class).fromBinary(buf, hint);
            return new ShardingPrepare.Response(shardRoundId);
        }
    }

}
