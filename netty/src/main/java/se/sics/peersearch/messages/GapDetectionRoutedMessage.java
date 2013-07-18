package se.sics.peersearch.messages;

import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.msgs.DirectMsgNetty;
import se.sics.gvod.common.msgs.MessageEncodingException;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.msgs.RewriteableMsg;
import se.sics.peersearch.net.MessageFrameDecoder;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 7/5/13
 * Time: 5:48 PM
 */
public class GapDetectionRoutedMessage extends DirectMsgNetty.Oneway {
    private final GapDetectionMessage.Request message;

    public GapDetectionRoutedMessage(VodAddress source, VodAddress destination, GapDetectionMessage.Request message) {
        super(source, destination);
        this.message = message;
    }


    public GapDetectionMessage.Request getMessage() {
        return message;
    }

    @Override
    public int getSize() {
        return getHeaderSize()+1400;
    }

    @Override
    public RewriteableMsg copy() {
        return new GapDetectionRoutedMessage(vodSrc, vodDest, message);
    }

    @Override
    public ByteBuf toByteArray() throws MessageEncodingException {
        ByteBuf buffer = createChannelBufferWithHeader();
        buffer.writeBytes(message.toByteArray());
        return buffer;
    }

    @Override
    public byte getOpcode() {
        return MessageFrameDecoder.GAP_DETECTION_ROUTED;
    }
}