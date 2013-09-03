package se.sics.ms.messages;

import io.netty.buffer.ByteBuf;
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
public class LeaderSuspicionMessageFactory {
    public static class Request extends  RelayMsgNettyFactory.Request{

        private Request() {
        }

        public static LeaderSuspicionMessage.Request fromBuffer(ByteBuf buffer)
                throws MessageDecodingException {
            return (LeaderSuspicionMessage.Request)
                    new LeaderSuspicionMessageFactory.Request().decode(buffer);
        }


        @Override
        protected RewriteableMsg process(ByteBuf buffer) throws MessageDecodingException {
            VodAddress leader = UserTypesDecoderFactory.readVodAddress(buffer);
            return new LeaderSuspicionMessage.Request(gvodSrc, gvodDest, clientId, remoteId, timeoutId, leader);
        }
    }

    public static class Response extends RelayMsgNettyFactory.Response {

        private Response() {
        }

        public static LeaderSuspicionMessage.Response fromBuffer(ByteBuf buffer)
                throws MessageDecodingException {
            return (LeaderSuspicionMessage.Response)
                    new LeaderSuspicionMessageFactory.Response().decode(buffer);
        }


        @Override
        protected RewriteableMsg process(ByteBuf buffer) throws MessageDecodingException {
            boolean isSuspected = UserTypesDecoderFactory.readBoolean(buffer);
            VodAddress leader = UserTypesDecoderFactory.readVodAddress(buffer);
            return new LeaderSuspicionMessage.Response(gvodSrc, gvodDest, clientId, remoteId, nextDest, timeoutId, status, isSuspected, leader);
        }
    }
}
