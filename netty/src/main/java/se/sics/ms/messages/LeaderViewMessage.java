package se.sics.ms.messages;

import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.msgs.DirectMsgNetty;
import se.sics.gvod.common.msgs.MessageEncodingException;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.msgs.RewriteableMsg;
import se.sics.ms.net.ApplicationTypesEncoderFactory;
import se.sics.ms.net.MessageFrameDecoder;
import se.sics.ms.types.PeerDescriptor;

import java.security.PublicKey;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 7/8/13
 * Time: 10:48 AM
 */
public class LeaderViewMessage extends DirectMsgNetty.Oneway {
    private final PeerDescriptor leaderSearchDescriptor;
    private final Set<PeerDescriptor> searchDescriptors;
    private final PublicKey leaderPublicKey;

    public LeaderViewMessage(VodAddress source, VodAddress destination, PeerDescriptor leaderSearchDescriptor, Set<PeerDescriptor> searchDescriptors, PublicKey leaderPublicKey) {
        super(source, destination);
        this.searchDescriptors = searchDescriptors;
        this.leaderSearchDescriptor = leaderSearchDescriptor;
        this.leaderPublicKey = leaderPublicKey;
    }

    public PeerDescriptor getLeaderSearchDescriptor() {
        return leaderSearchDescriptor;
    }

    public Set<PeerDescriptor> getSearchDescriptors() {
        return searchDescriptors;
    }

    public PublicKey getLeaderPublicKey() {
        return leaderPublicKey;
    }

    @Override
    public int getSize() {
        return getHeaderSize();
    }

    @Override
    public RewriteableMsg copy() {
        return new LeaderViewMessage(vodSrc, vodDest, leaderSearchDescriptor, searchDescriptors, leaderPublicKey);
    }

    @Override
    public ByteBuf toByteArray() throws MessageEncodingException {
        ByteBuf buffer = createChannelBufferWithHeader();
//        UserTypesEncoderFactory.writeVodNodeDescriptor(buffer, SearchDescriptor.toVodDescriptor(leaderSearchDescriptor));
        ApplicationTypesEncoderFactory.writeSearchDescriptor(buffer, leaderSearchDescriptor);
        ApplicationTypesEncoderFactory.writeSearchDescriptorSet(buffer, searchDescriptors);
        ApplicationTypesEncoderFactory.writePublicKey(buffer, leaderPublicKey);
        return buffer;
    }

    @Override
    public byte getOpcode() {
        return MessageFrameDecoder.VOTING_RESULT_MESSAGE;
    }
}
