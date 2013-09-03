package se.sics.ms.messages;

import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.VodDescriptor;
import se.sics.gvod.common.msgs.MessageDecodingException;
import se.sics.gvod.common.msgs.DirectMsgNettyFactory;
import se.sics.gvod.net.util.UserTypesDecoderFactory;

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
        VodDescriptor leader = UserTypesDecoderFactory.readVodNodeDescriptor(buffer);
        return new LeaderDeathAnnouncementMessage(vodSrc, vodDest, leader);
    }
}