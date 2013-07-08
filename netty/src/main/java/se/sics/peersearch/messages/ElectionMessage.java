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
 * Time: 12:18 PM
 */
public class ElectionMessage {
    public static class Request extends RelayMsgNetty.Request {
        private final int voteID;

        public Request(VodAddress source, VodAddress destination, int clientId, int remoteId, TimeoutId timeoutId, int voteID) {
            super(source, destination, clientId, remoteId, timeoutId);
            this.voteID = voteID;
        }

        public int getVoteID() {
            return voteID;
        }

        @Override
        public RewriteableMsg copy() {
            return new Request(vodSrc, vodDest, clientId, remoteId, timeoutId, voteID);
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
            buffer.writeInt(voteID);
            return buffer;
        }
    }

    public static class Response extends RelayMsgNetty.Response {
        private final int voteId;
        private final boolean isConvereged;
        private final boolean vote;
        private final VodAddress highest;

        public Response(VodAddress source, VodAddress destination, int clientId, int remoteId, VodAddress nextDest, TimeoutId timeoutId, RelayMsgNetty.Status status, int voteId, boolean convereged, boolean vote, VodAddress highest) {
            super(source, destination, clientId, remoteId, nextDest, timeoutId, status);
            this.voteId = voteId;
            isConvereged = convereged;
            this.vote = vote;
            this.highest = highest;
        }

        public int getVoteId() {
            return voteId;
        }

        public boolean isConvereged() {
            return isConvereged;
        }

        public boolean isVote() {
            return vote;
        }

        public VodAddress getHighest() {
            return highest;
        }

        @Override
        public RewriteableMsg copy() {
            return new Response(vodSrc, vodDest, clientId, remoteId, nextDest, timeoutId, getStatus(), voteId, isConvereged, vote, highest);
        }

        @Override
        public ChannelBuffer toByteArray() throws MessageEncodingException {
            ChannelBuffer buffer = createChannelBufferWithHeader();
            buffer.writeInt(voteId);
            UserTypesEncoderFactory.writeBoolean(buffer, isConvereged);
            UserTypesEncoderFactory.writeBoolean(buffer, vote);
            UserTypesEncoderFactory.writeVodAddress(buffer, highest);
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
