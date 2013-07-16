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
 * Date: 7/8/13
 * Time: 5:36 PM
 */
public class IndexDisseminationMessage extends DirectMsgNetty{
    private final long index;

    public IndexDisseminationMessage(VodAddress source, VodAddress destination, long index) {
        super(source, destination);
        this.index = index;
    }

    public long getIndex() {
        return index;
    }

    @Override
    public int getSize() {
        return getHeaderSize()+8;
    }

    @Override
    public RewriteableMsg copy() {
        return new IndexDisseminationMessage(vodSrc, vodDest, index);
    }

    @Override
    public ByteBuf toByteArray() throws MessageEncodingException {
        ByteBuf buffer = createChannelBufferWithHeader();
        buffer.writeLong(index);
        return buffer;
    }

    @Override
    public byte getOpcode() {
        return MessageFrameDecoder.INDEX_DESSIMINATION_MESSAGE;
    }
}
