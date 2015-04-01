package se.sics.p2ptoolbox.election.core.net;

import io.netty.buffer.ByteBuf;
import org.javatuples.Pair;
import se.sics.gvod.net.VodAddress;
import se.sics.p2ptoolbox.election.core.data.Promise;
import se.sics.p2ptoolbox.election.core.msg.LeaderPromiseMessage;
import se.sics.p2ptoolbox.serialization.SerializationContext;
import se.sics.p2ptoolbox.serialization.msg.HeaderField;
import se.sics.p2ptoolbox.serialization.serializer.NetContentMsgSerializer;

import java.util.Map;
import java.util.UUID;

/**
 * Serializer for the Request/Response messages that are exchanged between the node 
 * that is trying to assert itself as leader and the nodes which are being considered to be in its leader group.
 *
 * Created by babbarshaer on 2015-04-02.
 */
public class PromiseMessageSerializer {
    
   public static class Request extends NetContentMsgSerializer.Request<LeaderPromiseMessage.Request>{

       @Override
       public LeaderPromiseMessage.Request decode(SerializationContext context, ByteBuf buf) throws SerializerException, SerializationContext.MissingException {
           
           Pair<UUID, Map<String, HeaderField>> absReq = decodeAbsRequest(context, buf);
           Promise.Request content = context.getSerializer(Promise.Request.class).decode(context, buf);
           return new LeaderPromiseMessage.Request(new VodAddress(dummyHack, -1), new VodAddress(dummyHack, -1), absReq.getValue0(), content);
       }
   }
    
    public static class Response extends NetContentMsgSerializer.Response<LeaderPromiseMessage.Response>{

        @Override
        public LeaderPromiseMessage.Response decode(SerializationContext context, ByteBuf buf) throws SerializerException, SerializationContext.MissingException {
            
            Pair<UUID, Map<String, HeaderField>> absReq = decodeAbsResponse(context, buf);
            Promise.Response content = context.getSerializer(Promise.Response.class).decode(context, buf);
            return new LeaderPromiseMessage.Response(new VodAddress(dummyHack, -1), new VodAddress(dummyHack, -1), absReq.getValue0(), content);
        }
    }
    
}
