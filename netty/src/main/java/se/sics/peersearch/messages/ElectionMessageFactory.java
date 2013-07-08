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
            int counter = buffer.readInt();
            return new ElectionMessage.Request(gvodSrc, gvodDest, clientId, remoteId, timeoutId, counter);
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
            int voteId = buffer.readInt();
            boolean isConvereged = UserTypesDecoderFactory.readBoolean(buffer);
            boolean vote = UserTypesDecoderFactory.readBoolean(buffer);
            VodAddress highest = UserTypesDecoderFactory.readVodAddress(buffer);
            return new ElectionMessage.Response(gvodSrc, gvodDest, clientId, remoteId, nextDest, timeoutId, status, voteId, isConvereged, vote, highest);
        }
    }
}
