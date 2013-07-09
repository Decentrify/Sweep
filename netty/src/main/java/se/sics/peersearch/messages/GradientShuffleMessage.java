package se.sics.peersearch.messages;

import org.jboss.netty.buffer.ChannelBuffer;
import se.sics.gvod.common.msgs.MessageEncodingException;
import se.sics.gvod.common.msgs.DirectMsgNetty;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.msgs.RewriteableMsg;
import se.sics.gvod.net.msgs.RewriteableRetryTimeout;
import se.sics.gvod.net.msgs.ScheduleRetryTimeout;
import se.sics.gvod.timer.TimeoutId;
import se.sics.peersearch.net.ApplicationTypesEncoderFactory;
import se.sics.peersearch.net.MessageFrameDecoder;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 7/2/13
 * Time: 12:23 PM
 */
public class GradientShuffleMessage {
    public static class Request extends DirectMsgNetty {
        public static final int MAX_RESULTS_STR_LEN = 1400;

        private final VodAddress[] addresses;

        public VodAddress[] getAddresses() {
            return addresses;
        }

        public Request(VodAddress source, VodAddress destination, TimeoutId timeoutId, VodAddress[] addresses) {
            super(source, destination, timeoutId);
            this.addresses = addresses;
        }

        @Override
        public int getSize() {
            return getHeaderSize() + MAX_RESULTS_STR_LEN;
        }

        @Override
        public RewriteableMsg copy() {
            return new Request(vodSrc, vodDest, timeoutId, addresses);
        }

        @Override
        public ChannelBuffer toByteArray() throws MessageEncodingException {
            ChannelBuffer buffer = createChannelBufferWithHeader();
            ApplicationTypesEncoderFactory.writeVodAddressArray(buffer, addresses);
            return buffer;
        }

        @Override
        public byte getOpcode() {
            return MessageFrameDecoder.GRADIENT_SHUFFLE_REQUEST;
        }
    }

    public static class Response extends DirectMsgNetty {
        public static final int MAX_RESULTS_STR_LEN = 1400;

        private final VodAddress[] addresses;

        public VodAddress[] getAddresses() {
            return addresses;
        }

        public Response(VodAddress source, VodAddress destination, TimeoutId timeoutId, VodAddress[] addresses) {
            super(source, destination, timeoutId);
            this.addresses = addresses;
        }

        @Override
        public int getSize() {
            return getHeaderSize()+MAX_RESULTS_STR_LEN;
        }

        @Override
        public RewriteableMsg copy() {
            return new Response(vodSrc, vodDest, timeoutId, addresses);
        }

        @Override
        public ChannelBuffer toByteArray() throws MessageEncodingException {
            ChannelBuffer buffer = createChannelBufferWithHeader();
            ApplicationTypesEncoderFactory.writeVodAddressArray(buffer, addresses);
            return buffer;
        }

        @Override
        public byte getOpcode() {
            return MessageFrameDecoder.GRADIENT_SHUFFLE_RESPONSE;
        }
    }

    public static class RequestTimeout extends RewriteableRetryTimeout {

        public RequestTimeout(ScheduleRetryTimeout st, RewriteableMsg retryMessage) {
            super(st, retryMessage);
        }
    }
}
