package se.sics.peersearch.messages;

import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.msgs.DirectMsgNetty;
import se.sics.gvod.common.msgs.MessageEncodingException;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.msgs.RewriteableMsg;
import se.sics.gvod.net.msgs.RewriteableRetryTimeout;
import se.sics.gvod.net.msgs.ScheduleRetryTimeout;
import se.sics.gvod.timer.TimeoutId;
import se.sics.peersearch.net.ApplicationTypesEncoderFactory;
import se.sics.peersearch.net.MessageFrameDecoder;

/**
 * @author Steffen Grohsschmiedt
 */
public class LeaderLookupMessage {
    public static class Request extends DirectMsgNetty.Request {

        public Request(VodAddress source, VodAddress destination, TimeoutId timeoutId) {
            super(source, destination, timeoutId);
        }

        @Override
        public int getSize() {
            return getHeaderSize();
        }

        @Override
        public RewriteableMsg copy() {
            return new Request(vodSrc, vodDest, timeoutId);
        }

        @Override
        public ByteBuf toByteArray() throws MessageEncodingException {
            ByteBuf buffer = createChannelBufferWithHeader();
            return buffer;
        }

        @Override
        public byte getOpcode() {
            return MessageFrameDecoder.LEADER_LOOKUP_REQUEST;
        }
    }

    public static class Response extends DirectMsgNetty.Response {
        public static final int MAX_RESULTS_STR_LEN = 1400;

        private final VodAddress[] addresses;

        public Response(VodAddress source, VodAddress destination, TimeoutId timeoutId, VodAddress[] addresses) {
            super(source, destination, timeoutId);
            this.addresses = addresses;
        }

        public VodAddress[] getAddresses() {
            return addresses;
        }

        @Override
        public int getSize() {
            // TODO check this
            return getHeaderSize() + MAX_RESULTS_STR_LEN;
        }

        @Override
        public RewriteableMsg copy() {
            return  new Response(vodSrc, vodDest, timeoutId, addresses);
        }

        @Override
        public ByteBuf toByteArray() throws MessageEncodingException {
            ByteBuf buffer = createChannelBufferWithHeader();
            ApplicationTypesEncoderFactory.writeVodAddressArray(buffer, addresses);
            return buffer;
        }

        @Override
        public byte getOpcode() {
            return MessageFrameDecoder.LEADER_LOOKUP_RESPONSE;
        }
    }

    public static class RequestTimeout extends RewriteableRetryTimeout {

        public RequestTimeout(ScheduleRetryTimeout st, RewriteableMsg retryMessage) {
            super(st, retryMessage);
        }
    }
}
