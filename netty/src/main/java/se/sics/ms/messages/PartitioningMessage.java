package se.sics.ms.messages;

import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.msgs.DirectMsgNetty;
import se.sics.gvod.common.msgs.MessageEncodingException;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.msgs.RewriteableMsg;
import se.sics.gvod.net.util.UserTypesEncoderFactory;
import se.sics.gvod.timer.TimeoutId;
import se.sics.ms.net.MessageFrameDecoder;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 8/20/13
 * Time: 11:11 AM
 */
public class PartitioningMessage extends DirectMsgNetty.Oneway {
    private final long middleEntryId;
    private final TimeoutId requestId;

    public PartitioningMessage(VodAddress source, VodAddress destination, long middleEntryId, TimeoutId requestId) {
        super(source, destination);
        this.middleEntryId = middleEntryId;
        this.requestId = requestId;
    }

    public long getMiddleEntryId() {
        return middleEntryId;
    }

    public TimeoutId getRequestId() {
        return requestId;
    }

    @Override
    public int getSize() {
        return getHeaderSize() + 8;
    }

    @Override
    public RewriteableMsg copy() {
        return new PartitioningMessage(vodSrc, vodDest, middleEntryId, requestId);
    }

    @Override
    public ByteBuf toByteArray() throws MessageEncodingException {
        ByteBuf buffer = createChannelBufferWithHeader();
        buffer.writeLong(middleEntryId);
        UserTypesEncoderFactory.writeTimeoutId(buffer, requestId);
        return buffer;
    }

    @Override
    public byte getOpcode() {
        return MessageFrameDecoder.PARTITIONING_MESSAGE;
    }
}
