package se.sics.peersearch.messages;

import org.jboss.netty.buffer.ChannelBuffer;
import se.sics.gvod.common.msgs.MessageEncodingException;
import se.sics.gvod.common.msgs.RelayMsgNetty;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.msgs.RewriteableMsg;
import se.sics.gvod.net.msgs.RewriteableRetryTimeout;
import se.sics.gvod.net.msgs.ScheduleRetryTimeout;
import se.sics.gvod.net.util.UserTypesEncoderFactory;
import se.sics.gvod.timer.TimeoutId;
import se.sics.peersearch.net.MessageFrameDecoder;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 7/2/13
 * Time: 1:01 PM
 */
public class LeaderSuspectionMessage {
    public static class Request extends RelayMsgNetty.Request {

        /**
         * Creates a message for leader suspection
         * @param source
         * @param destination
         * @param clientId
         * @param remoteId
         * @param timeoutId
         */
        public Request(VodAddress source, VodAddress destination, int clientId, int remoteId, TimeoutId timeoutId) {
            super(source, destination, clientId, remoteId, timeoutId);
        }

        @Override
        public byte getOpcode() {
            return MessageFrameDecoder.LEADER_SUSPECTION_REQUEST;
        }

        @Override
        public RewriteableMsg copy() {
            return new Request(vodSrc, vodDest, clientId, remoteId, timeoutId);
        }
    }

    public static class Response extends RelayMsgNetty.Response {
        private final boolean isSuspected;

        public Response(VodAddress source, VodAddress destination, int clientId, int remoteId, VodAddress nextDest, TimeoutId timeoutId, RelayMsgNetty.Status status, boolean isSuspected) {
            super(source, destination, clientId, remoteId, nextDest, timeoutId, status);
            this.isSuspected = isSuspected;
        }

        public boolean isSuspected() {
            return isSuspected;
        }

        @Override
        public int getSize() {
            return super.getSize() + 1;
        }

        @Override
        public ChannelBuffer toByteArray() throws MessageEncodingException {
            ChannelBuffer buffer = createChannelBufferWithHeader();
            UserTypesEncoderFactory.writeBoolean(buffer, isSuspected);
            return buffer;
        }

        @Override
        public byte getOpcode() {
            return MessageFrameDecoder.LEADER_SUSPECTION_RESPONSE;
        }

        @Override
        public RewriteableMsg copy() {
            return new Response(vodSrc, vodDest, clientId, remoteId, nextDest, timeoutId, getStatus(), isSuspected);
        }
    }

    public static class RequestTimeout extends RewriteableRetryTimeout {

        public RequestTimeout(ScheduleRetryTimeout st, RewriteableMsg retryMessage) {
            super(st, retryMessage);
        }
    }
}
