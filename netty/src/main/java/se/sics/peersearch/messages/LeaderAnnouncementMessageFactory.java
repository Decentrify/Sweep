package se.sics.peersearch.messages;

import org.jboss.netty.buffer.ChannelBuffer;
import se.sics.gvod.common.msgs.MessageDecodingException;
import se.sics.gvod.common.msgs.VodMsgNettyFactory;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.util.UserTypesDecoderFactory;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 7/2/13
 * Time: 12:34 PM
 */
public class LeaderAnnouncementMessageFactory extends VodMsgNettyFactory {
    public static LeaderAnnouncementMessage fromBuffer(ChannelBuffer buffer)
            throws MessageDecodingException {
        return (LeaderAnnouncementMessage)
                new LeaderAnnouncementMessageFactory().decode(buffer, false);
    }

    @Override
    protected LeaderAnnouncementMessage process(ChannelBuffer buffer) throws MessageDecodingException {
        VodAddress leader = UserTypesDecoderFactory.readVodAddress(buffer);
        return new LeaderAnnouncementMessage(vodSrc, vodDest, leader);
    }
}