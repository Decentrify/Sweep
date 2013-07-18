package se.sics.peersearch.messages;

import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.msgs.DirectMsgNettyFactory;
import se.sics.gvod.common.msgs.MessageDecodingException;
import se.sics.gvod.net.msgs.DirectMsg;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 7/5/13
 * Time: 12:28 PM
 */
public class AddIndexEntryRoutedMessageFactory extends DirectMsgNettyFactory.Oneway {
    private AddIndexEntryRoutedMessageFactory() {
    }

    public static AddIndexEntryRoutedMessage fromBuffer(ByteBuf buffer)
            throws MessageDecodingException {
        return (AddIndexEntryRoutedMessage)
                new AddIndexEntryRoutedMessageFactory().decode(buffer, false);
    }
    @Override
    protected DirectMsg process(ByteBuf buffer) throws MessageDecodingException {
        buffer.readByte();
        AddIndexEntryMessage.Request request = AddIndexEntryMessageFactory.Request.fromBuffer(buffer);
        return new AddIndexEntryRoutedMessage(vodSrc, vodDest, request);
    }
}
