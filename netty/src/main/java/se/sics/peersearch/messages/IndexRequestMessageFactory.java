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
 * Time: 5:26 PM
 */
public class IndexRequestMessageFactory extends DirectMsgNettyFactory {

    private IndexRequestMessageFactory() {
    }

    public static IndexRequestMessage fromBuffer(ByteBuf buffer)
            throws MessageDecodingException {
        return (IndexRequestMessage)
                new IndexRequestMessageFactory().decode(buffer, true);
    }

    @Override
    protected IndexRequestMessage process(ByteBuf buffer) throws MessageDecodingException {
        long index = buffer.readLong();
        VodAddress leaderAddress = UserTypesDecoderFactory.readVodAddress(buffer);
        return new IndexRequestMessage(vodSrc, vodDest,
                timeoutId, index, leaderAddress);
    }

}
