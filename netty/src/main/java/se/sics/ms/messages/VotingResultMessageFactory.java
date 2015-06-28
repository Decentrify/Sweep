package se.sics.ms.messages;

import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.msgs.DirectMsgNettyFactory;
import se.sics.gvod.common.msgs.MessageDecodingException;
import se.sics.gvod.net.msgs.DirectMsg;
import se.sics.ms.net.ApplicationTypesDecoderFactory;
import se.sics.ms.types.PeerDescriptor;

import java.security.PublicKey;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 7/8/13
 * Time: 10:54 AM
 */
public class VotingResultMessageFactory extends DirectMsgNettyFactory.Oneway {

    private VotingResultMessageFactory() {
    }

    public static LeaderViewMessage fromBuffer(ByteBuf buffer) throws MessageDecodingException {
        return (LeaderViewMessage) new VotingResultMessageFactory().decode(buffer);
    }

    @Override
    protected DirectMsg process(ByteBuf buffer) throws MessageDecodingException {
//        SearchDescriptor searchDescriptor = new SearchDescriptor(UserTypesDecoderFactory.readVodNodeDescriptor(buffer));
        PeerDescriptor searchDescriptor = ApplicationTypesDecoderFactory.readSearchDescriptor(buffer);
        Set<PeerDescriptor> view = ApplicationTypesDecoderFactory.readSearchDescriptorSet(buffer);
        PublicKey pub = ApplicationTypesDecoderFactory.readPublicKey(buffer);

        return new LeaderViewMessage(vodSrc, vodDest, searchDescriptor, view, pub);
    }
}