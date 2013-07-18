package se.sics.peersearch.messages;

import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.msgs.DirectMsgNettyFactory;
import se.sics.gvod.common.msgs.MessageDecodingException;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 7/8/13
 * Time: 5:39 PM
 */
public class IndexDisseminationMessageFactory extends DirectMsgNettyFactory.Oneway {

    private IndexDisseminationMessageFactory() {
    }

    public static IndexDisseminationMessage fromBuffer(ByteBuf buffer)
            throws MessageDecodingException {
        return (IndexDisseminationMessage)
                new IndexDisseminationMessageFactory().decode(buffer, false);
    }

    @Override
    protected IndexDisseminationMessage process(ByteBuf buffer) throws MessageDecodingException {
        long index = buffer.readLong();
        return new IndexDisseminationMessage(vodSrc, vodDest, index);
    }

}