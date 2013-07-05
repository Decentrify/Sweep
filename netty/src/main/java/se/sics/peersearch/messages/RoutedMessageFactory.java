package se.sics.peersearch.messages;

import org.jboss.netty.buffer.ChannelBuffer;
import se.sics.gvod.common.msgs.DirectMsgNettyFactory;
import se.sics.gvod.common.msgs.MessageDecodingException;
import se.sics.gvod.net.msgs.DirectMsg;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 7/5/13
 * Time: 12:28 PM
 */
public class RoutedMessageFactory extends DirectMsgNettyFactory {
    private RoutedMessageFactory() {
    }

    public static RoutedMessage fromBuffer(ChannelBuffer buffer)
            throws MessageDecodingException {
        return (RoutedMessage)
                new RoutedMessageFactory().decode(buffer, false);
    }
    @Override
    protected DirectMsg process(ChannelBuffer buffer) throws MessageDecodingException {
        AddIndexEntryMessage.Request request = AddIndexEntryMessageFactory.Request.fromBuffer(buffer);
        return new RoutedMessage(vodSrc, vodDest, request);
    }
}
