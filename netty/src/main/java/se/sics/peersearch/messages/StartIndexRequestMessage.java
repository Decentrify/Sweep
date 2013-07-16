package se.sics.peersearch.messages;

import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.msgs.DirectMsgNetty;
import se.sics.gvod.common.msgs.MessageEncodingException;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.msgs.RewriteableMsg;
import se.sics.gvod.timer.TimeoutId;
import se.sics.peersearch.net.MessageFrameDecoder;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 7/8/13
 * Time: 5:03 PM
 */
public class StartIndexRequestMessage extends DirectMsgNetty {

    public StartIndexRequestMessage(VodAddress source, VodAddress destination, TimeoutId timeoutId) {
        super(source, destination, timeoutId);
    }

    @Override
    public int getSize() {
        return getHeaderSize()+8;
    }

    @Override
    public RewriteableMsg copy() {
        return new StartIndexRequestMessage(vodSrc, vodDest, timeoutId);
    }

    @Override
    public ByteBuf toByteArray() throws MessageEncodingException {
        ByteBuf buffer = createChannelBufferWithHeader();
        return buffer;
    }

    @Override
    public byte getOpcode() {
        return MessageFrameDecoder.START_INDEX_REQUEST_MESSAGE;
    }
}
