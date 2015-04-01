package se.sics.p2ptoolbox.election.core.net;

import io.netty.buffer.ByteBuf;
import org.javatuples.Pair;
import se.sics.gvod.net.VodAddress;
import se.sics.p2ptoolbox.election.core.data.ExtensionRequest;
import se.sics.p2ptoolbox.election.core.msg.LeaderExtensionRequest;
import se.sics.p2ptoolbox.serialization.SerializationContext;
import se.sics.p2ptoolbox.serialization.Serializer;
import se.sics.p2ptoolbox.serialization.msg.HeaderField;
import se.sics.p2ptoolbox.serialization.serializer.NetContentMsgSerializer;

import java.util.Map;
import java.util.UUID;

/**
 * Serializer for the extension request message.
 * Created by babbarshaer on 2015-04-02.
 */
public class ExtensionMessageSerializer  extends NetContentMsgSerializer.OneWay<LeaderExtensionRequest>{

    @Override
    public LeaderExtensionRequest decode(SerializationContext context, ByteBuf buf) throws SerializerException, SerializationContext.MissingException {
        Pair<UUID, Map<String, HeaderField>> absReq = decodeAbsOneWay(context, buf);
        ExtensionRequest content = context.getSerializer(ExtensionRequest.class).decode(context, buf);
        return new LeaderExtensionRequest(new VodAddress(dummyHack, -1), new VodAddress(dummyHack, -1), absReq.getValue0(), content);
    }
}
