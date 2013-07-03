package se.sics.peersearch.messages;

import org.jboss.netty.buffer.ChannelBuffer;
import se.sics.gvod.common.msgs.MessageEncodingException;
import se.sics.gvod.common.msgs.DirectMsgNetty;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.msgs.RewriteableMsg;
import se.sics.gvod.net.util.UserTypesEncoderFactory;
import se.sics.peersearch.net.MessageFrameDecoder;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 7/2/13
 * Time: 12:32 PM
 */
public class LeaderAnnouncementMessage extends DirectMsgNetty {
    private final VodAddress leader;

    /**
     * Creates a new LeaderAnnouncementMsg
     * @param source
     * @param destination
     * @param leader VodAddress of the leader
     */
    public LeaderAnnouncementMessage(VodAddress source, VodAddress destination, VodAddress leader) {
        super(source, destination);
        this.leader = leader;
    }

    /**
     * Returns the leader
     * @return leader's VodAddress
     */
    public VodAddress getLeader() {
        return leader;
    }

    @Override
    public int getSize() {
        return getHeaderSize() + 54;
    }

    @Override
    public RewriteableMsg copy() {
        return new LeaderAnnouncementMessage(vodSrc, vodDest, leader);
    }

    @Override
    public ChannelBuffer toByteArray() throws MessageEncodingException {
        ChannelBuffer buffer = createChannelBufferWithHeader();
        UserTypesEncoderFactory.writeVodAddress(buffer, leader);
        return buffer;
    }

    @Override
    public byte getOpcode() {
        return MessageFrameDecoder.LEADER_ANNOUNCEMENT;
    }
}
