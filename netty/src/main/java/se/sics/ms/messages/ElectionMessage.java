package se.sics.ms.messages;

import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.VodDescriptor;
import se.sics.gvod.common.msgs.DirectMsgNetty;
import se.sics.gvod.common.msgs.MessageEncodingException;
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
 * Time: 12:18 PM
 */
public class ElectionMessage {
    public static class Request extends DirectMsgNetty.Request {
        private final int voteID;
        private final VodDescriptor leaderCandicateDescriptor;

        public Request(VodAddress source, VodAddress destination, TimeoutId timeoutId, int voteID, VodDescriptor leaderCandidateDescriptor) {
            super(source, destination, timeoutId);

            this.voteID = voteID;
            this.leaderCandicateDescriptor = leaderCandidateDescriptor;
        }

        public int getVoteID() {
            return voteID;
        }

        public VodDescriptor getLeaderCandidateDescriptor() {
            return leaderCandicateDescriptor;
        }

        @Override
        public RewriteableMsg copy() {
            return new Request(vodSrc, vodDest, timeoutId, voteID, leaderCandicateDescriptor);
        }

        @Override
        public int getSize() {
            return super.getHeaderSize();
        }

        @Override
        public byte getOpcode() {
            return MessageFrameDecoder.LEADER_SELECTION_REQUEST;
        }

        @Override
        public ByteBuf toByteArray() throws MessageEncodingException {
            ByteBuf buffer = createChannelBufferWithHeader();
            buffer.writeInt(voteID);
            UserTypesEncoderFactory.writeVodNodeDescriptor(buffer, leaderCandicateDescriptor);
            return buffer;
        }
    }

    public static class Response extends DirectMsgNetty.Response {
        private final int voteId;
        private final boolean isConvereged;
        private final boolean vote;
        private final VodDescriptor highestUtilityNode;

        public Response(VodAddress source, VodAddress destination, TimeoutId timeoutId, int voteId, boolean converged, boolean vote, VodDescriptor highestUtilityNode) {
            super(source, destination, timeoutId);
            this.voteId = voteId;
            isConvereged = converged;
            this.vote = vote;
            this.highestUtilityNode = highestUtilityNode;
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

        public VodDescriptor getHighestUtilityNode() {
            return highestUtilityNode;
        }

        @Override
        public RewriteableMsg copy() {
            return new Response(vodSrc, vodDest, timeoutId, voteId, isConvereged, vote, highestUtilityNode);
        }

        @Override
        public ByteBuf toByteArray() throws MessageEncodingException {
            ByteBuf buffer = createChannelBufferWithHeader();
            buffer.writeInt(voteId);
            UserTypesEncoderFactory.writeBoolean(buffer, isConvereged);
            UserTypesEncoderFactory.writeBoolean(buffer, vote);
            UserTypesEncoderFactory.writeVodNodeDescriptor(buffer, highestUtilityNode);
            return buffer;
        }

        @Override
        public byte getOpcode() {
            return MessageFrameDecoder.LEADER_SELECTION_RESPONSE;
        }

        @Override
        public int getSize() {
            return super.getHeaderSize();
        }
    }

    public static class RequestTimeout extends RewriteableRetryTimeout {
        public RequestTimeout(ScheduleRetryTimeout st, RewriteableMsg retryMessage) {
            super(st, retryMessage);
        }
    }
}
