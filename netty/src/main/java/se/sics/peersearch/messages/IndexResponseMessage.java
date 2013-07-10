package se.sics.peersearch.messages;

import org.jboss.netty.buffer.ChannelBuffer;
import se.sics.gvod.common.msgs.DirectMsgNetty;
import se.sics.gvod.common.msgs.MessageEncodingException;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.msgs.RewriteableMsg;
import se.sics.gvod.net.util.UserTypesEncoderFactory;
import se.sics.gvod.timer.UUID;
import se.sics.peersearch.net.MessageFrameDecoder;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 7/10/13
 * Time: 1:47 PM
 */
public class IndexResponseMessage extends DirectMsgNetty{
    private final long index;
    private final UUID messageId;

    public IndexResponseMessage(VodAddress source, VodAddress destination, long index, UUID messageId) {
        super(source, destination);
        this.index = index;
        this.messageId = messageId;
    }

    public long getIndex() {
        return index;
    }

    public UUID getMessageId() {
        return messageId;
    }

    @Override
    public int getSize() {
        return getHeaderSize()+8+4;
    }

    @Override
    public RewriteableMsg copy() {
        return new IndexResponseMessage(vodSrc, vodDest, index, messageId);
    }

    @Override
    public ChannelBuffer toByteArray() throws MessageEncodingException {
        ChannelBuffer buffer = createChannelBufferWithHeader();
        buffer.writeLong(index);
        UserTypesEncoderFactory.writeTimeoutId(buffer, messageId);
        return buffer;
    }

    @Override
    public byte getOpcode() {
        return MessageFrameDecoder.INDEX_RESPONSE_MESSAGE;
    }
}
