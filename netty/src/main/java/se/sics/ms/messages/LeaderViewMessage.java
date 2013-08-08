package se.sics.ms.messages;

import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.VodDescriptor;
import se.sics.gvod.common.msgs.DirectMsgNetty;
import se.sics.gvod.common.msgs.MessageEncodingException;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.msgs.RewriteableMsg;
import se.sics.gvod.net.util.UserTypesEncoderFactory;
import se.sics.ms.net.ApplicationTypesEncoderFactory;
import se.sics.ms.net.MessageFrameDecoder;

import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 7/8/13
 * Time: 10:48 AM
 */
public class LeaderViewMessage extends DirectMsgNetty.Oneway {
    private final VodDescriptor leaderVodDescriptor;
    private final Set<VodDescriptor> vodDescriptors;

    public LeaderViewMessage(VodAddress source, VodAddress destination, VodDescriptor leaderVodDescriptor, Set<VodDescriptor> vodDescriptors) {
        super(source, destination);
        this.vodDescriptors = vodDescriptors;
        this.leaderVodDescriptor = leaderVodDescriptor;
    }

    public VodDescriptor getLeaderVodDescriptor() {
        return leaderVodDescriptor;
    }

    public Set<VodDescriptor> getVodDescriptors() {
        return vodDescriptors;
    }

    @Override
    public int getSize() {
        return getHeaderSize();
    }

    @Override
    public RewriteableMsg copy() {
        return new LeaderViewMessage(vodSrc, vodDest, leaderVodDescriptor, vodDescriptors);
    }

    @Override
    public ByteBuf toByteArray() throws MessageEncodingException {
        ByteBuf buffer = createChannelBufferWithHeader();
        UserTypesEncoderFactory.writeVodNodeDescriptor(buffer, leaderVodDescriptor);
        ApplicationTypesEncoderFactory.writeVodDescriptorSet(buffer, vodDescriptors);
        return buffer;
    }

    @Override
    public byte getOpcode() {
        return MessageFrameDecoder.VOTING_RESULT_MESSAGE;
    }
}
