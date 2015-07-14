package se.sics.ms.messages;

import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.msgs.DirectMsgNettyFactory;
import se.sics.gvod.common.msgs.MessageDecodingException;
import se.sics.ms.net.ApplicationTypesDecoderFactory;
import se.sics.ms.types.PeerDescriptor;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 7/2/13
 * Time: 12:34 PM
 */
public class LeaderDeathAnnouncementMessageFactory extends DirectMsgNettyFactory.Oneway {
    public static LeaderDeathAnnouncementMessage fromBuffer(ByteBuf buffer)
            throws MessageDecodingException {
        return (LeaderDeathAnnouncementMessage)
                new LeaderDeathAnnouncementMessageFactory().decode(buffer);
    }

    @Override
    protected LeaderDeathAnnouncementMessage process(ByteBuf buffer) throws MessageDecodingException {
//        SearchDescriptor leader = new SearchDescriptor(UserTypesDecoderFactory.readVodNodeDescriptor(buffer));
        PeerDescriptor leader = ApplicationTypesDecoderFactory.readSearchDescriptor(buffer);
        return new LeaderDeathAnnouncementMessage(vodSrc, vodDest, leader);
    }
}