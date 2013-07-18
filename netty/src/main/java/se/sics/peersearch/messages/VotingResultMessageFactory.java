package se.sics.peersearch.messages;

import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.msgs.DirectMsgNettyFactory;
import se.sics.gvod.common.msgs.MessageDecodingException;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.msgs.DirectMsg;
import se.sics.peersearch.net.ApplicationTypesDecoderFactory;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 7/8/13
 * Time: 10:54 AM
 */
public class VotingResultMessageFactory extends DirectMsgNettyFactory.Oneway {

    private VotingResultMessageFactory() {
    }

    public static VotingResultMessage fromBuffer(ByteBuf buffer)
            throws MessageDecodingException {
        return (VotingResultMessage)
                new VotingResultMessageFactory().decode(buffer, false);
    }

    @Override
    protected DirectMsg process(ByteBuf buffer) throws MessageDecodingException {
        VodAddress[] view = ApplicationTypesDecoderFactory.readVodAddressArray(buffer);
        return new VotingResultMessage(vodSrc, vodDest, view);
    }
}