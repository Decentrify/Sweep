package se.sics.peersearch.messages;

import org.jboss.netty.buffer.ChannelBuffer;
import se.sics.gvod.common.msgs.MessageDecodingException;
import se.sics.gvod.common.msgs.VodMsgNettyFactory;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.msgs.VodMsg;
import se.sics.peersearch.net.ApplicationTypesDecoderFactory;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 7/2/13
 * Time: 12:26 PM
 */
public class GradientShuffleMessageFactory {
    public static class Request extends VodMsgNettyFactory {

        private Request() {
        }

        public static GradientShuffleMessage.Request fromBuffer(ChannelBuffer buffer)
                throws MessageDecodingException {
            return (GradientShuffleMessage.Request)
                    new GradientShuffleMessageFactory.Request().decode(buffer, true);
        }

        @Override
        protected GradientShuffleMessage.Request process(ChannelBuffer buffer) throws MessageDecodingException {
            VodAddress[] addresses = ApplicationTypesDecoderFactory.readVodAddressArray(buffer);
            return new GradientShuffleMessage.Request(vodSrc, vodDest,
                    timeoutId, addresses);
        }

    }

    public static class Response extends VodMsgNettyFactory {

        private Response() {
        }

        public static GradientShuffleMessage.Response fromBuffer(ChannelBuffer buffer)
                throws MessageDecodingException {
            return (GradientShuffleMessage.Response)
                    new GradientShuffleMessageFactory.Response().decode(buffer, true);
        }

        @Override
        protected VodMsg process(ChannelBuffer buffer) throws MessageDecodingException {
            VodAddress[] addresses = ApplicationTypesDecoderFactory.readVodAddressArray(buffer);
            return new GradientShuffleMessage.Response(vodSrc, vodDest, timeoutId, addresses);
        }
    }
}