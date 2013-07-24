package se.sics.peersearch.messages;

import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.msgs.DirectMsgNettyFactory;
import se.sics.gvod.common.msgs.MessageDecodingException;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.msgs.DirectMsg;
import se.sics.gvod.net.util.UserTypesDecoderFactory;
import se.sics.peersearch.net.ApplicationTypesDecoderFactory;
import se.sics.peersearch.types.IndexEntry;

/**
 *
 * @author Steffen Grohsschmiedt
 */
public class LeaderLookupMessageFactory {
    public static class Request extends DirectMsgNettyFactory.Request {

        private Request() {
        }

        public static LeaderLookupMessage.Request fromBuffer(ByteBuf buffer) throws MessageDecodingException {
            return (LeaderLookupMessage.Request) new LeaderLookupMessageFactory.Request().decode(buffer, true);
        }

        @Override
        protected LeaderLookupMessage.Request process(ByteBuf buffer) throws MessageDecodingException {
            return new LeaderLookupMessage.Request(vodSrc, vodDest, timeoutId);
        }

    }

    public static class Response extends DirectMsgNettyFactory.Response {

        private Response() {
        }

        public static LeaderLookupMessage.Response fromBuffer(ByteBuf buffer) throws MessageDecodingException {
            return (LeaderLookupMessage.Response) new LeaderLookupMessageFactory.Response().decode(buffer, true);
        }

        @Override
        protected DirectMsg process(ByteBuf buffer) throws MessageDecodingException {
            boolean terminated = buffer.readBoolean();
            VodAddress[] items = ApplicationTypesDecoderFactory.readVodAddressArray(buffer);
            return new LeaderLookupMessage.Response(vodSrc, vodDest, timeoutId, terminated, items);
        }
    }
}
