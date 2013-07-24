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
    public static final int A = 3;
    public static final int K = 5;

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

        private final boolean leader;
        private final VodAddress[] addresses;

        public Response(VodAddress source, VodAddress destination, TimeoutId timeoutId, boolean leader, VodAddress[] addresses) {
            super(source, destination, timeoutId);
            this.leader = leader;
            this.addresses = addresses;
        }

        @Override
        public int getSize() {
            // TODO check this
            return getHeaderSize() + MAX_RESULTS_STR_LEN;
        }

        public boolean isLeader() {
            return leader;
        }

        public VodAddress[] getAddresses() {
            return addresses;
        }

        @Override
        public RewriteableMsg copy() {
            return  new Response(vodSrc, vodDest, timeoutId, leader, addresses);
        }

        @Override
        public ByteBuf toByteArray() throws MessageEncodingException {
            ByteBuf buffer = createChannelBufferWithHeader();
            buffer.writeBoolean(leader);
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
