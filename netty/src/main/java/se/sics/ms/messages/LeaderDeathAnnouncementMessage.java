package se.sics.ms.messages;

import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.msgs.DirectMsgNetty;
import se.sics.gvod.common.msgs.MessageEncodingException;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.msgs.RewriteableMsg;
import se.sics.ms.net.ApplicationTypesEncoderFactory;
import se.sics.ms.net.MessageFrameDecoder;
import se.sics.ms.types.PeerDescriptor;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 7/2/13
 * Time: 12:32 PM
 */
public class LeaderDeathAnnouncementMessage extends DirectMsgNetty.Oneway {
    private final PeerDescriptor leader;

    /**
     * Creates a new LeaderAnnouncementMsg
     * @param source
     * @param destination
     * @param leader SearchDescriptor of the leader
     */
    public LeaderDeathAnnouncementMessage(VodAddress source, VodAddress destination, PeerDescriptor leader) {
        super(source, destination);

        if(leader == null)
            throw new NullPointerException("leader can't be null");

        this.leader = leader;
    }

    /**
     * Returns the leader
     * @return leader's VodAddress
     */
    public PeerDescriptor getLeader() {
        return leader;
    }

    @Override
    public int getSize() {
        return getHeaderSize() + 54;
    }

    @Override
    public RewriteableMsg copy() {
        return new LeaderDeathAnnouncementMessage(vodSrc, vodDest, leader);
    }

    @Override
    public ByteBuf toByteArray() throws MessageEncodingException {
        ByteBuf buffer = createChannelBufferWithHeader();
//        UserTypesEncoderFactory.writeVodNodeDescriptor(buffer, SearchDescriptor.toVodDescriptor(leader));
        ApplicationTypesEncoderFactory.writeSearchDescriptor(buffer, leader);
        return buffer;
    }

    @Override
    public byte getOpcode() {
        return MessageFrameDecoder.LEADER_ANNOUNCEMENT;
    }
}
