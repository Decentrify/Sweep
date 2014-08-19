package se.sics.ms.messages;

import io.netty.buffer.ByteBuf;
import se.sics.ms.types.SearchDescriptor;
import se.sics.gvod.common.msgs.DirectMsgNettyFactory;
import se.sics.gvod.common.msgs.MessageDecodingException;
import se.sics.gvod.net.util.UserTypesDecoderFactory;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 7/8/13
 * Time: 12:30 PM
 */
public class RejectLeaderMessageFactory extends DirectMsgNettyFactory.Oneway {

    private RejectLeaderMessageFactory() {
    }

    public static RejectLeaderMessage fromBuffer(ByteBuf buffer)
            throws MessageDecodingException {
        return (RejectLeaderMessage)
                new RejectLeaderMessageFactory().decode(buffer);
    }

    @Override
    protected RejectLeaderMessage process(ByteBuf buffer) throws MessageDecodingException {
        SearchDescriptor betterNode = new SearchDescriptor(UserTypesDecoderFactory.readVodNodeDescriptor(buffer));
        return new RejectLeaderMessage(vodSrc, vodDest,betterNode);
    }

}