package se.sics.peersearch.messages;

import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.msgs.DirectMsgNetty;
import se.sics.gvod.common.msgs.MessageEncodingException;
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
 * Date: 7/8/13
 * Time: 11:15 AM
 */
public class RejectFollowerMessage {
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
            return MessageFrameDecoder.REJECT_FOLLOWER_REQUEST;
        }
    }

    public static class Response extends DirectMsgNetty.Response {
        private final boolean isInView;

        public Response(VodAddress source, VodAddress destination, TimeoutId timeoutId, boolean inView) {
            super(source, destination, timeoutId);
            isInView = inView;
        }

        public boolean isInView() {
            return isInView;
        }

        @Override
        public int getSize() {
            return getHeaderSize()+1;
        }

        @Override
        public RewriteableMsg copy() {
            return new Response(vodSrc, vodDest, timeoutId, isInView);
        }

        @Override
        public ByteBuf toByteArray() throws MessageEncodingException {
            ByteBuf buffer = createChannelBufferWithHeader();
            UserTypesEncoderFactory.writeBoolean(buffer, isInView);
            return buffer;
        }

        @Override
        public byte getOpcode() {
            return MessageFrameDecoder.REJECT_FOLLOWER_RESPONSE;
        }
    }

    public static class RequestTimeout extends RewriteableRetryTimeout {
        public RequestTimeout(ScheduleRetryTimeout st, RewriteableMsg retryMessage) {
            super(st, retryMessage);
        }
    }
}
