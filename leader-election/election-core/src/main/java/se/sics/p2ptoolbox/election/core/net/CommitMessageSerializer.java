package se.sics.p2ptoolbox.election.core.net;

import io.netty.buffer.ByteBuf;
import org.javatuples.Pair;
import se.sics.gvod.net.VodAddress;
import se.sics.p2ptoolbox.election.core.data.LeaseCommit;
import se.sics.p2ptoolbox.election.core.msg.LeaseCommitMessage;
import se.sics.p2ptoolbox.serialization.SerializationContext;
import se.sics.p2ptoolbox.serialization.msg.HeaderField;
import se.sics.p2ptoolbox.serialization.serializer.NetContentMsgSerializer;

import java.util.Map;
import java.util.UUID;

/**
 * Serializer for the Lease Commit Message Sent by the Leader to all the nodes that it thinks that
 * should be in the leader group.
 *
 * Created by babbarshaer on 2015-04-02.
 */
public class CommitMessageSerializer extends NetContentMsgSerializer.OneWay<LeaseCommitMessage>{
    
    @Override
    public LeaseCommitMessage decode(SerializationContext context, ByteBuf buf) throws SerializerException, SerializationContext.MissingException {
        
        Pair<UUID, Map<String, HeaderField>> absReq = decodeAbsOneWay(context, buf);
        LeaseCommit content = context.getSerializer(LeaseCommit.class).decode(context, buf);
        return new LeaseCommitMessage(new VodAddress(dummyHack, -1), new VodAddress(dummyHack, -1), absReq.getValue0(), content);
    }
}
