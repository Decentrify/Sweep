package se.sics.p2ptoolbox.election.core.net;

import io.netty.buffer.ByteBuf;
import org.javatuples.Pair;
import se.sics.gvod.net.VodAddress;
import se.sics.p2ptoolbox.election.core.data.LeaseCommitUpdated;
import se.sics.p2ptoolbox.election.core.data.Promise;
import se.sics.p2ptoolbox.election.core.msg.LeaderPromiseMessage;
import se.sics.p2ptoolbox.election.core.msg.LeaseCommitMessageUpdated;
import se.sics.p2ptoolbox.serialization.SerializationContext;
import se.sics.p2ptoolbox.serialization.Serializer;
import se.sics.p2ptoolbox.serialization.msg.HeaderField;
import se.sics.p2ptoolbox.serialization.serializer.NetContentMsgSerializer;

import java.util.Map;
import java.util.UUID;

/**
 * Serializer for the Lease Commit Message.
 *
 * Created by babbar on 2015-04-09.
 */
public class LeaseCommitMessageUpdatedSerializer {


    public static class Request extends NetContentMsgSerializer.Request<LeaseCommitMessageUpdated.Request>{

        @Override
        public LeaseCommitMessageUpdated.Request decode(SerializationContext context, ByteBuf buf) throws SerializerException, SerializationContext.MissingException {
            
            Pair<UUID, Map<String, HeaderField>> absReq = decodeAbsRequest(context, buf);
            LeaseCommitUpdated.Request content = context.getSerializer(LeaseCommitUpdated.Request.class).decode(context, buf);
            return new LeaseCommitMessageUpdated.Request(new VodAddress(dummyHack, -1), new VodAddress(dummyHack, -1), absReq.getValue0(), content);
        }
    }

    public static class Response extends NetContentMsgSerializer.Response<LeaseCommitMessageUpdated.Response>{

        @Override
        public LeaseCommitMessageUpdated.Response decode(SerializationContext context, ByteBuf buf) throws SerializerException, SerializationContext.MissingException {

            Pair<UUID, Map<String, HeaderField>> absReq = decodeAbsResponse(context, buf);
            LeaseCommitUpdated.Response content = context.getSerializer(LeaseCommitUpdated.Response.class).decode(context, buf);
            return new LeaseCommitMessageUpdated.Response(new VodAddress(dummyHack, -1), new VodAddress(dummyHack, -1), absReq.getValue0(), content);
            
        }
    }

}
