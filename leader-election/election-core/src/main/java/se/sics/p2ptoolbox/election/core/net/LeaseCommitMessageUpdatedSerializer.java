package se.sics.p2ptoolbox.election.core.net;

import io.netty.buffer.ByteBuf;
import se.sics.p2ptoolbox.election.core.msg.LeaseCommitMessageUpdated;
import se.sics.p2ptoolbox.serialization.SerializationContext;
import se.sics.p2ptoolbox.serialization.Serializer;
import se.sics.p2ptoolbox.serialization.serializer.NetContentMsgSerializer;

/**
 * Created by babbar on 2015-04-09.
 */
public class LeaseCommitMessageUpdatedSerializer {


    public static class Request extends NetContentMsgSerializer.Request<LeaseCommitMessageUpdated.Request>{

        @Override
        public LeaseCommitMessageUpdated.Request decode(SerializationContext serializationContext, ByteBuf byteBuf) throws SerializerException, SerializationContext.MissingException {
            return null;
        }
    }

    public static class Response extends NetContentMsgSerializer.Response<LeaseCommitMessageUpdated.Response>{

        @Override
        public LeaseCommitMessageUpdated.Response decode(SerializationContext serializationContext, ByteBuf byteBuf) throws SerializerException, SerializationContext.MissingException {
            return null;
        }
    }

}
