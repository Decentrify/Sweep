package se.sics.peersearch.messages;

import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.msgs.DirectMsgNettyFactory;
import se.sics.gvod.common.msgs.MessageDecodingException;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 7/8/13
 * Time: 5:06 PM
 */
public class StartIndexRequestMessageFactory extends DirectMsgNettyFactory {

    private StartIndexRequestMessageFactory() {
    }

    public static StartIndexRequestMessage fromBuffer(ByteBuf buffer)
            throws MessageDecodingException {
        return (StartIndexRequestMessage)
                new StartIndexRequestMessageFactory().decode(buffer, true);
    }

    @Override
    protected StartIndexRequestMessage process(ByteBuf buffer) throws MessageDecodingException {
        return new StartIndexRequestMessage(vodSrc, vodDest,
                timeoutId);
    }

}
