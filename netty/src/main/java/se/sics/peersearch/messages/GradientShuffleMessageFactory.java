package se.sics.peersearch.messages;

import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.msgs.MessageDecodingException;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.common.msgs.DirectMsgNettyFactory;
import se.sics.gvod.net.msgs.DirectMsg;
import se.sics.gvod.net.util.UserTypesDecoderFactory;
import se.sics.gvod.timer.UUID;
import se.sics.peersearch.net.ApplicationTypesDecoderFactory;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 7/2/13
 * Time: 12:26 PM
 */
public class GradientShuffleMessageFactory {
    public static class Request extends DirectMsgNettyFactory {

        private Request() {
        }

        public static GradientShuffleMessage.Request fromBuffer(ByteBuf buffer)
                throws MessageDecodingException {
            return (GradientShuffleMessage.Request)
                    new GradientShuffleMessageFactory.Request().decode(buffer, true);
        }

        @Override
        protected GradientShuffleMessage.Request process(ByteBuf buffer) throws MessageDecodingException {
            VodAddress[] addresses = ApplicationTypesDecoderFactory.readVodAddressArray(buffer);
            return new GradientShuffleMessage.Request(vodSrc, vodDest,
                    timeoutId, addresses);
        }

    }

    public static class Response extends DirectMsgNettyFactory {

        private Response() {
        }

        public static GradientShuffleMessage.Response fromBuffer(ByteBuf buffer)
                throws MessageDecodingException {
            return (GradientShuffleMessage.Response)
                    new GradientShuffleMessageFactory.Response().decode(buffer, true);
        }

        @Override
        protected DirectMsg process(ByteBuf buffer) throws MessageDecodingException {
            VodAddress[] addresses = ApplicationTypesDecoderFactory.readVodAddressArray(buffer);
            return new GradientShuffleMessage.Response(vodSrc, vodDest, timeoutId, addresses);
        }
    }
}