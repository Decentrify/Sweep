package se.sics.peersearch.messages;

import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.msgs.MessageEncodingException;
import se.sics.gvod.common.msgs.DirectMsgNetty;
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
 * Time: 1:07 PM
 */
public class HeartbeatMessage {
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
            return MessageFrameDecoder.HEARTBEAT_REQUEST;
        }
    }

    public static class Response extends DirectMsgNetty.Response {

        public Response(VodAddress source, VodAddress destination, TimeoutId timeoutId) {
            super(source, destination, timeoutId);
        }

        @Override
        public int getSize() {
            return getHeaderSize();
        }

        @Override
        public RewriteableMsg copy() {
            return new Response(vodSrc, vodDest, timeoutId);
        }

        @Override
        public ByteBuf toByteArray() throws MessageEncodingException {
            ByteBuf buffer = createChannelBufferWithHeader();
            return buffer;
        }

        @Override
        public byte getOpcode() {
            return MessageFrameDecoder.HEARTBEAT_RESPONSE;
        }
    }

    public static class RequestTimeout extends RewriteableRetryTimeout {
        public RequestTimeout(ScheduleRetryTimeout st, RewriteableMsg retryMessage) {
            super(st, retryMessage);
        }
    }
}
