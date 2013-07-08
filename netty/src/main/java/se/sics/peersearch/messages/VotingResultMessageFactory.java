package se.sics.peersearch.messages;

import org.jboss.netty.buffer.ChannelBuffer;
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
public class VotingResultMessageFactory extends DirectMsgNettyFactory {

    private VotingResultMessageFactory() {
    }

    public static VotingResultMessage fromBuffer(ChannelBuffer buffer)
            throws MessageDecodingException {
        return (VotingResultMessage)
                new VotingResultMessageFactory().decode(buffer, false);
    }

    @Override
    protected DirectMsg process(ChannelBuffer buffer) throws MessageDecodingException {
        VodAddress[] view = ApplicationTypesDecoderFactory.readVodAddressArray(buffer);
        return new VotingResultMessage(vodSrc, vodDest, view);
    }
}