package se.sics.ms.messages;

import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.msgs.MessageEncodingException;
import se.sics.gvod.common.msgs.RelayMsgNetty;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.msgs.RewriteableMsg;
import se.sics.gvod.net.msgs.RewriteableRetryTimeout;
import se.sics.gvod.net.msgs.ScheduleRetryTimeout;
import se.sics.gvod.net.util.UserTypesEncoderFactory;
import se.sics.gvod.timer.TimeoutId;
import se.sics.ms.net.MessageFrameDecoder;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 7/2/13
 * Time: 1:01 PM
 */
public class LeaderSuspicionMessage {
    public static class Request extends RelayMsgNetty.Request {
        private final VodAddress leader;

        /**
         * Creates a message for leader suspection
         * @param source
         * @param destination
         * @param clientId
         * @param remoteId
         * @param timeoutId
         * @param leader
         */
        public Request(VodAddress source, VodAddress destination, int clientId, int remoteId, TimeoutId timeoutId, VodAddress leader) {
            super(source, destination, clientId, remoteId, timeoutId);

            if(leader == null)
                throw new NullPointerException("leader can't be null");

            this.leader = leader;
        }

        public VodAddress getLeader() {
            return leader;
        }

        @Override
        public byte getOpcode() {
            return MessageFrameDecoder.LEADER_SUSPICION_REQUEST;
        }

        @Override
        public RewriteableMsg copy() {
            return new Request(vodSrc, vodDest, clientId, remoteId, timeoutId, leader);
        }

        @Override
        public ByteBuf toByteArray() throws MessageEncodingException {
            ByteBuf buffer = createChannelBufferWithHeader();
            UserTypesEncoderFactory.writeVodAddress(buffer, leader);
            return buffer;
        }
    }

    public static class Response extends RelayMsgNetty.Response {
        private final boolean isSuspected;
        private final VodAddress leader;

        public Response(VodAddress source, VodAddress destination, int clientId, int remoteId, VodAddress nextDest, TimeoutId timeoutId, RelayMsgNetty.Status status, boolean isSuspected, VodAddress leader) {
            super(source, destination, clientId, remoteId, nextDest, timeoutId, status);
            this.isSuspected = isSuspected;
            this.leader = leader;
        }

        public boolean isSuspected() {
            return isSuspected;
        }

        public VodAddress getLeader() {
            return leader;
        }

        @Override
        public int getSize() {
            return super.getSize() + 1;
        }

        @Override
        public ByteBuf toByteArray() throws MessageEncodingException {
            ByteBuf buffer = createChannelBufferWithHeader();
            UserTypesEncoderFactory.writeBoolean(buffer, isSuspected);
            UserTypesEncoderFactory.writeVodAddress(buffer, leader);
            return buffer;
        }

        @Override
        public byte getOpcode() {
            return MessageFrameDecoder.LEADER_SUSPICION_RESPONSE;
        }

        @Override
        public RewriteableMsg copy() {
            return new Response(vodSrc, vodDest, clientId, remoteId, nextDest, timeoutId, getStatus(), isSuspected, leader);
        }
    }
}
