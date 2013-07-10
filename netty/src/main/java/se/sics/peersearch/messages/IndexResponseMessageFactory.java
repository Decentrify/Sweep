package se.sics.peersearch.messages;

import org.jboss.netty.buffer.ChannelBuffer;
import se.sics.gvod.common.msgs.DirectMsgNettyFactory;
import se.sics.gvod.common.msgs.MessageDecodingException;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.util.UserTypesDecoderFactory;
import se.sics.gvod.timer.UUID;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 7/10/13
 * Time: 1:51 PM
 */
public class IndexResponseMessageFactory extends DirectMsgNettyFactory {

    private IndexResponseMessageFactory() {
    }

    public static IndexResponseMessage fromBuffer(ChannelBuffer buffer)
            throws MessageDecodingException {
        return (IndexResponseMessage)
                new IndexResponseMessageFactory().decode(buffer, false);
    }

    @Override
    protected IndexResponseMessage process(ChannelBuffer buffer) throws MessageDecodingException {
        long index = buffer.readLong();
        UUID id = (UUID)UserTypesDecoderFactory.readTimeoutId(buffer);
        return new IndexResponseMessage(vodSrc, vodDest, index, id);
    }

}