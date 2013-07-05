package se.sics.peersearch.messages;

import org.jboss.netty.buffer.ChannelBuffer;
import se.sics.gvod.common.msgs.DirectMsgNetty;
import se.sics.gvod.common.msgs.MessageEncodingException;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.msgs.RewriteableMsg;
import se.sics.peersearch.net.MessageFrameDecoder;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 7/5/13
 * Time: 12:21 PM
 */
public class AddIndexEntryRoutedMessage extends DirectMsgNetty {
    private final AddIndexEntryMessage.Request message;

    public AddIndexEntryRoutedMessage(VodAddress source, VodAddress destination, AddIndexEntryMessage.Request message) {
        super(source, destination);
        this.message = message;
    }


    public AddIndexEntryMessage.Request getMessage() {
        return message;
    }

    @Override
    public int getSize() {
        return getHeaderSize()+1400;
    }

    @Override
    public RewriteableMsg copy() {
        return new AddIndexEntryRoutedMessage(vodSrc, vodDest, message);
    }

    @Override
    public ChannelBuffer toByteArray() throws MessageEncodingException {
        ChannelBuffer buffer = createChannelBufferWithHeader();
        buffer.writeBytes(message.toByteArray());
        return buffer;
    }

    @Override
    public byte getOpcode() {
        return MessageFrameDecoder.ADD_INDEX_ENTRY_ROUTED;
    }
}
