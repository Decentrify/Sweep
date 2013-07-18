package se.sics.peersearch.messages;

import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.msgs.DirectMsgNettyFactory;
import se.sics.gvod.common.msgs.MessageDecodingException;
import se.sics.gvod.net.msgs.DirectMsg;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 7/5/13
 * Time: 5:49 PM
 */
public class GapDetectionRoutedMessageFactory extends DirectMsgNettyFactory.Oneway {
    private GapDetectionRoutedMessageFactory() {
    }

    public static GapDetectionRoutedMessage fromBuffer(ByteBuf buffer)
            throws MessageDecodingException {
        return (GapDetectionRoutedMessage)
                new GapDetectionRoutedMessageFactory().decode(buffer, false);
    }
    @Override
    protected DirectMsg process(ByteBuf buffer) throws MessageDecodingException {
        buffer.readByte();
        GapDetectionMessage.Request request = GapDetectionMessageFactory.Request.fromBuffer(buffer);
        return new GapDetectionRoutedMessage(vodSrc, vodDest, request);
    }
}
