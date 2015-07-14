package se.sics.ms.serializer;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.ms.data.ShardingCommit;

import java.util.UUID;

/**
 * Serializer wrapper for the message exchange during the
 * shard commit phase of the sharding protocol.
 *
 * Created by babbar on 2015-06-22.
 */
public class ShardCommitSerializer {



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

            ShardingCommit.Request request = (ShardingCommit.Request)o;
            Serializers.lookupSerializer(UUID.class).toBinary(request.getShardRoundId(), buf);
        }

        @Override
        public Object fromBinary(ByteBuf buf, Optional<Object> hint) {

            UUID shardRoundId = (UUID)Serializers.lookupSerializer(UUID.class).fromBinary(buf, hint);
            return new ShardingCommit.Request(shardRoundId);
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

            ShardingCommit.Response response = (ShardingCommit.Response)o;
            Serializers.lookupSerializer(UUID.class).toBinary(response.getShardRoundId(), buf);

        }

        @Override
        public Object fromBinary(ByteBuf buf, Optional<Object> hint) {

            UUID shardRoundId = (UUID)Serializers.lookupSerializer(UUID.class).fromBinary(buf, hint);
            return new ShardingCommit.Response(shardRoundId);

        }
    }



}
