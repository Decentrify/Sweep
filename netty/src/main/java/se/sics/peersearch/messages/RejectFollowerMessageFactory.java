package se.sics.peersearch.messages;

import org.jboss.netty.buffer.ChannelBuffer;
import se.sics.gvod.common.msgs.DirectMsgNettyFactory;
import se.sics.gvod.common.msgs.MessageDecodingException;
import se.sics.gvod.net.msgs.DirectMsg;
import se.sics.gvod.net.util.UserTypesDecoderFactory;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 7/8/13
 * Time: 11:23 AM
 */
public class RejectFollowerMessageFactory {
    public static class Request extends DirectMsgNettyFactory {

        private Request() {
        }

        public static RejectFollowerMessage.Request fromBuffer(ChannelBuffer buffer)
                throws MessageDecodingException {
            return (RejectFollowerMessage.Request)
                    new RejectFollowerMessageFactory.Request().decode(buffer, true);
        }

        @Override
        protected RejectFollowerMessage.Request process(ChannelBuffer buffer) throws MessageDecodingException {
            return new RejectFollowerMessage.Request(vodSrc, vodDest,
                    timeoutId);
        }

    }

    public static class Response extends DirectMsgNettyFactory {

        private Response() {
        }

        public static RejectFollowerMessage.Response fromBuffer(ChannelBuffer buffer)
                throws MessageDecodingException {
            return (RejectFollowerMessage.Response)
                    new RejectFollowerMessageFactory.Response().decode(buffer, true);
        }

        @Override
        protected DirectMsg process(ChannelBuffer buffer) throws MessageDecodingException {
            boolean isInView = UserTypesDecoderFactory.readBoolean(buffer);
            return new RejectFollowerMessage.Response(vodSrc, vodDest, timeoutId, isInView);
        }
    }
}