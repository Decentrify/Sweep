package se.sics.peersearch.messages;

import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.msgs.DirectMsgNettyFactory;
import se.sics.gvod.common.msgs.MessageDecodingException;
import se.sics.gvod.net.VodAddress;
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
                new RejectLeaderMessageFactory().decode(buffer, false);
    }

    @Override
    protected RejectLeaderMessage process(ByteBuf buffer) throws MessageDecodingException {
        VodAddress betterNode = UserTypesDecoderFactory.readVodAddress(buffer);
        return new RejectLeaderMessage(vodSrc, vodDest,betterNode);
    }

}