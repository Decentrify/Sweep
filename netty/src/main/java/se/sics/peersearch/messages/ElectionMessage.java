package se.sics.peersearch.messages;

import org.jboss.netty.buffer.ChannelBuffer;
import se.sics.gvod.common.msgs.MessageEncodingException;
import se.sics.gvod.common.msgs.RelayMsgNetty;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.msgs.RewriteableMsg;
import se.sics.gvod.net.msgs.RewriteableRetryTimeout;
import se.sics.gvod.net.msgs.ScheduleRetryTimeout;
import se.sics.gvod.timer.TimeoutId;
import se.sics.peersearch.net.MessageFrameDecoder;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 7/2/13
 * Time: 12:18 PM
 */
public class ElectionMessage {
    public static class Request extends RelayMsgNetty.Request {

        public Request(VodAddress source, VodAddress destination, int clientId, int remoteId, TimeoutId timeoutId) {
            super(source, destination, clientId, remoteId, timeoutId);
        }

        @Override
        public RewriteableMsg copy() {
            return new Request(vodSrc, vodDest, clientId, remoteId, timeoutId);
        }

        @Override
        public int getSize() {
            return super.getSize();
        }

        @Override
        public byte getOpcode() {
            return MessageFrameDecoder.LEADER_SELECTION_REQUEST;
        }

        @Override
        public ChannelBuffer toByteArray() throws MessageEncodingException {
            ChannelBuffer buffer = createChannelBufferWithHeader();
            return buffer;
        }
    }

    public static class Response extends RelayMsgNetty.Response {

        public Response(VodAddress source, VodAddress destination, int clientId, int remoteId, VodAddress nextDest, TimeoutId timeoutId, RelayMsgNetty.Status status) {
            super(source, destination, clientId, remoteId, nextDest, timeoutId, status);
        }

        @Override
        public RewriteableMsg copy() {
            return new Response(vodSrc, vodDest, clientId, remoteId, nextDest, timeoutId, getStatus());
        }

        @Override
        public ChannelBuffer toByteArray() throws MessageEncodingException {
            ChannelBuffer buffer = createChannelBufferWithHeader();
            return buffer;
        }

        @Override
        public byte getOpcode() {
            return MessageFrameDecoder.LEADER_SELECTION_RESPONSE;
        }

        @Override
        public int getSize() {
            return super.getSize();
        }
    }

    public static class RequestTimeout extends RewriteableRetryTimeout {
        public RequestTimeout(ScheduleRetryTimeout st, RewriteableMsg retryMessage) {
            super(st, retryMessage);
        }
    }
}
