package se.sics.peersearch.messages;

import org.jboss.netty.buffer.ChannelBuffer;
import se.sics.gvod.common.msgs.MessageDecodingException;
import se.sics.gvod.common.msgs.RelayMsgNettyFactory;
import se.sics.gvod.net.msgs.RewriteableMsg;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 7/2/13
 * Time: 12:20 PM
 */
public class ElectionMessageFactory {
    public static class Request extends  RelayMsgNettyFactory.Request{

        private Request() {
        }

        public static ElectionMessage.Request fromBuffer(ChannelBuffer buffer)
                throws MessageDecodingException {
            return (ElectionMessage.Request)
                    new ElectionMessageFactory.Request().decode(buffer, true);
        }


        @Override
        protected RewriteableMsg process(ChannelBuffer buffer) throws MessageDecodingException {
            return new ElectionMessage.Request(gvodSrc, gvodDest, clientId, remoteId, timeoutId);
        }
    }

    public static class Response extends RelayMsgNettyFactory.Response {

        private Response() {
        }

        public static ElectionMessage.Response fromBuffer(ChannelBuffer buffer)
                throws MessageDecodingException {
            return (ElectionMessage.Response)
                    new ElectionMessageFactory.Response().decode(buffer, true);
        }


        @Override
        protected RewriteableMsg process(ChannelBuffer buffer) throws MessageDecodingException {
            return new ElectionMessage.Response(gvodSrc, gvodDest, clientId, remoteId, nextDest, timeoutId, status);
        }
    }
}
