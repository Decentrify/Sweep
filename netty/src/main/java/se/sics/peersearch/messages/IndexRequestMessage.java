package se.sics.peersearch.messages;

import org.jboss.netty.buffer.ChannelBuffer;
import se.sics.gvod.common.msgs.DirectMsgNetty;
import se.sics.gvod.common.msgs.MessageEncodingException;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.msgs.RewriteableMsg;
import se.sics.gvod.net.util.UserTypesEncoderFactory;
import se.sics.gvod.timer.TimeoutId;
import se.sics.peersearch.net.MessageFrameDecoder;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 7/8/13
 * Time: 5:23 PM
 */
public class IndexRequestMessage extends DirectMsgNetty{
    private final long index;
    private final VodAddress leaderAddress;

    public IndexRequestMessage(VodAddress source, VodAddress destination, TimeoutId timeoutId, long index, VodAddress leaderAddress) {
        super(source, destination, timeoutId);
        this.index = index;
        this.leaderAddress = leaderAddress;
    }

    public long getIndex() {
        return index;
    }

    public VodAddress getLeaderAddress() {
        return leaderAddress;
    }

    @Override
    public int getSize() {
        return getHeaderSize()+8+16;
    }

    @Override
    public RewriteableMsg copy() {
        return new IndexRequestMessage(vodSrc, vodDest, timeoutId, index, leaderAddress);
    }

    @Override
    public ChannelBuffer toByteArray() throws MessageEncodingException {
        ChannelBuffer buffer = createChannelBufferWithHeader();
        buffer.writeLong(index);
        UserTypesEncoderFactory.writeVodAddress(buffer, leaderAddress);
        return buffer;
    }

    @Override
    public byte getOpcode() {
        return MessageFrameDecoder.INDEX_REQUEST_MESSAGE;
    }
}
