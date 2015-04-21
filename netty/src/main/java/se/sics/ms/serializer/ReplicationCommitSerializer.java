package se.sics.ms.serializer;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.msgs.MessageDecodingException;
import se.sics.gvod.common.msgs.MessageEncodingException;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.ms.data.ReplicationCommit;
import se.sics.ms.helper.SerializerDecoderHelper;
import se.sics.ms.helper.SerializerEncoderHelper;

import java.util.UUID;

/**
 * Container for the serializers for the replication commit phase of the Entry addition protocol.
 * Created by babbar on 2015-04-21.
 */
public class ReplicationCommitSerializer {


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

            try {
                ReplicationCommit.Request request = (ReplicationCommit.Request)o;
                Serializers.lookupSerializer(UUID.class).toBinary(request.getCommitRoundId(), byteBuf);
                byteBuf.writeLong(request.getEntryId());
                SerializerEncoderHelper.writeStringLength65536(byteBuf, request.getSignature());
            }
            catch (MessageEncodingException e) {
                e.printStackTrace();
                throw new RuntimeException(e.getMessage());
            }
        }

        @Override
        public Object fromBinary(ByteBuf byteBuf, Optional<Object> optional) {




            try{
                UUID commitRoundId = (UUID) Serializers.lookupSerializer(UUID.class).fromBinary(byteBuf, optional);
                long entryId = byteBuf.readLong();
                String signature = SerializerDecoderHelper.readStringLength65536(byteBuf);

                return new ReplicationCommit.Request(commitRoundId, entryId, signature);
            } catch (MessageDecodingException e) {
                e.printStackTrace();
                throw new RuntimeException(e.getMessage());
            }

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

        }

        @Override
        public Object fromBinary(ByteBuf byteBuf, Optional<Object> optional) {
            return null;
        }
    }

}
