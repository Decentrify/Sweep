package se.sics.ms.messages;

import io.netty.buffer.ByteBuf;
import se.sics.ms.types.SearchDescriptor;
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
    private final SearchDescriptor leaderSearchDescriptor;
    private final Set<SearchDescriptor> searchDescriptors;

    public LeaderViewMessage(VodAddress source, VodAddress destination, SearchDescriptor leaderSearchDescriptor, Set<SearchDescriptor> searchDescriptors) {
        super(source, destination);
        this.searchDescriptors = searchDescriptors;
        this.leaderSearchDescriptor = leaderSearchDescriptor;
    }

    public SearchDescriptor getLeaderSearchDescriptor() {
        return leaderSearchDescriptor;
    }

    public Set<SearchDescriptor> getSearchDescriptors() {
        return searchDescriptors;
    }

    @Override
    public int getSize() {
        return getHeaderSize();
    }

    @Override
    public RewriteableMsg copy() {
        return new LeaderViewMessage(vodSrc, vodDest, leaderSearchDescriptor, searchDescriptors);
    }

    @Override
    public ByteBuf toByteArray() throws MessageEncodingException {
        ByteBuf buffer = createChannelBufferWithHeader();
        UserTypesEncoderFactory.writeVodNodeDescriptor(buffer, SearchDescriptor.toVodDescriptor(leaderSearchDescriptor));
        ApplicationTypesEncoderFactory.writeSearchDescriptorSet(buffer, searchDescriptors);
        return buffer;
    }

    @Override
    public byte getOpcode() {
        return MessageFrameDecoder.VOTING_RESULT_MESSAGE;
    }
}
