package se.sics.ms.messages;

import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.msgs.DirectMsgNettyFactory;
import se.sics.gvod.common.msgs.MessageDecodingException;
import se.sics.gvod.net.msgs.DirectMsg;
import se.sics.ms.net.ApplicationTypesDecoderFactory;
import se.sics.ms.types.PeerDescriptor;

import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 7/2/13
 * Time: 12:26 PM
 */
public class GradientShuffleMessageFactory {
    public static class Request extends DirectMsgNettyFactory.Request {

        private Request() {
        }

        public static GradientShuffleMessage.Request fromBuffer(ByteBuf buffer)
                throws MessageDecodingException {
            return (GradientShuffleMessage.Request)
                    new GradientShuffleMessageFactory.Request().decode(buffer);
        }

        @Override
        protected GradientShuffleMessage.Request process(ByteBuf buffer) throws MessageDecodingException {
            Set<PeerDescriptor> searchDescriptors = ApplicationTypesDecoderFactory.readSearchDescriptorSet(buffer);
            return new GradientShuffleMessage.Request(vodSrc, vodDest, timeoutId, searchDescriptors);
        }

    }

    public static class Response extends DirectMsgNettyFactory.Response {

        private Response() {
        }

        public static GradientShuffleMessage.Response fromBuffer(ByteBuf buffer)
                throws MessageDecodingException {
            return (GradientShuffleMessage.Response)
                    new GradientShuffleMessageFactory.Response().decode(buffer);
        }

        @Override
        protected DirectMsg process(ByteBuf buffer) throws MessageDecodingException {
            Set<PeerDescriptor> searchDescriptors = ApplicationTypesDecoderFactory.readSearchDescriptorSet(buffer);
            return new GradientShuffleMessage.Response(vodSrc, vodDest, timeoutId, searchDescriptors);
        }
    }
}