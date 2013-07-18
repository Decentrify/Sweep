package se.sics.peersearch.messages;

import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.msgs.MessageDecodingException;
import se.sics.gvod.common.msgs.DirectMsgNettyFactory;
import se.sics.gvod.net.msgs.DirectMsg;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 7/2/13
 * Time: 1:11 PM
 */
public class HeartbeatMessageFactory {
    public static class Request extends  DirectMsgNettyFactory.Request {

        private Request() {
        }

        public static HeartbeatMessage.Request fromBuffer(ByteBuf buffer)
                throws MessageDecodingException {
            return (HeartbeatMessage.Request)
                    new HeartbeatMessageFactory.Request().decode(buffer, true);
        }


        @Override
        protected DirectMsg process(ByteBuf buffer) throws MessageDecodingException {
            return new HeartbeatMessage.Request(vodSrc, vodDest, timeoutId);
        }
    }

    public static class Response extends DirectMsgNettyFactory.Response {

        private Response() {
        }

        public static HeartbeatMessage.Response fromBuffer(ByteBuf buffer)
                throws MessageDecodingException {
            return (HeartbeatMessage.Response)
                    new HeartbeatMessageFactory.Response().decode(buffer, true);
        }


        @Override
        protected DirectMsg process(ByteBuf buffer) throws MessageDecodingException {
            return new HeartbeatMessage.Response(vodSrc, vodDest, timeoutId);
        }
    }
}
