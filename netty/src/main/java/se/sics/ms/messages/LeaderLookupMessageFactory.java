package se.sics.ms.messages;

import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.msgs.DirectMsgNettyFactory;
import se.sics.gvod.common.msgs.MessageDecodingException;
import se.sics.gvod.net.msgs.DirectMsg;
import se.sics.ms.net.ApplicationTypesDecoderFactory;
import se.sics.ms.types.PeerDescriptor;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Steffen Grohsschmiedt
 */
public class LeaderLookupMessageFactory {
    public static class Request extends DirectMsgNettyFactory.Request {

        private Request() {
        }

        public static LeaderLookupMessage.Request fromBuffer(ByteBuf buffer) throws MessageDecodingException {
            return (LeaderLookupMessage.Request) new LeaderLookupMessageFactory.Request().decode(buffer);
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
            return (LeaderLookupMessage.Response) new LeaderLookupMessageFactory.Response().decode(buffer);
        }

        @Override
        protected DirectMsg process(ByteBuf buffer) throws MessageDecodingException {
            boolean terminated = buffer.readBoolean();
            List<PeerDescriptor> items = new ArrayList<PeerDescriptor>(ApplicationTypesDecoderFactory.readSearchDescriptorSet(buffer));
            return new LeaderLookupMessage.Response(vodSrc, vodDest, timeoutId, terminated, items);
        }
    }
}
