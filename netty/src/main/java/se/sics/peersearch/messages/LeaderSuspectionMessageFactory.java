package se.sics.peersearch.messages;

import org.jboss.netty.buffer.ChannelBuffer;
import se.sics.gvod.common.msgs.MessageDecodingException;
import se.sics.gvod.common.msgs.RelayMsgNettyFactory;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.msgs.RewriteableMsg;
import se.sics.gvod.net.util.UserTypesDecoderFactory;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 7/2/13
 * Time: 1:02 PM
 */
public class LeaderSuspectionMessageFactory {
    public static class Request extends  RelayMsgNettyFactory.Request{

        private Request() {
        }

        public static LeaderSuspectionMessage.Request fromBuffer(ChannelBuffer buffer)
                throws MessageDecodingException {
            return (LeaderSuspectionMessage.Request)
                    new LeaderSuspectionMessageFactory.Request().decode(buffer, true);
        }


        @Override
        protected RewriteableMsg process(ChannelBuffer buffer) throws MessageDecodingException {
            VodAddress leader = UserTypesDecoderFactory.readVodAddress(buffer);
            return new LeaderSuspectionMessage.Request(gvodSrc, gvodDest, clientId, remoteId, timeoutId, leader);
        }
    }

    public static class Response extends RelayMsgNettyFactory.Response {

        private Response() {
        }

        public static LeaderSuspectionMessage.Response fromBuffer(ChannelBuffer buffer)
                throws MessageDecodingException {
            return (LeaderSuspectionMessage.Response)
                    new LeaderSuspectionMessageFactory.Response().decode(buffer, true);
        }


        @Override
        protected RewriteableMsg process(ChannelBuffer buffer) throws MessageDecodingException {
            boolean isSuspected = UserTypesDecoderFactory.readBoolean(buffer);
            VodAddress leader = UserTypesDecoderFactory.readVodAddress(buffer);
            return new LeaderSuspectionMessage.Response(gvodSrc, gvodDest, clientId, remoteId, nextDest, timeoutId, status, isSuspected, leader);
        }
    }
}
