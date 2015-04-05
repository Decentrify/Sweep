package se.sics.ms.messages;

import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.msgs.DirectMsgNetty;
import se.sics.gvod.common.msgs.MessageEncodingException;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.msgs.RewriteableMsg;
import se.sics.gvod.net.util.UserTypesEncoderFactory;
import se.sics.gvod.timer.ScheduleTimeout;
import se.sics.gvod.timer.TimeoutId;
import se.sics.ms.net.ApplicationTypesEncoderFactory;
import se.sics.ms.net.MessageFrameDecoder;
import se.sics.ms.timeout.IndividualTimeout;
import se.sics.ms.types.SearchDescriptor;

import java.util.HashSet;
import java.util.List;

/**
 * @author Steffen Grohsschmiedt
 */
public class LeaderLookupMessage {
    public static final int QueryLimit = 4;
    public static final int ResponseLimit = 8;

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
        private final List<SearchDescriptor> searchDescriptors;

        public Response(VodAddress source, VodAddress destination, TimeoutId timeoutId, boolean leader, List<SearchDescriptor> searchDescriptors) {
            super(source, destination, timeoutId);
            this.leader = leader;
            this.searchDescriptors = searchDescriptors;
        }

        @Override
        public int getSize() {
            // TODO check this
            return getHeaderSize() + MAX_RESULTS_STR_LEN;
        }

        public boolean isLeader() {
            return leader;
        }

        public List<SearchDescriptor> getSearchDescriptors() {
            return searchDescriptors;
        }

        @Override
        public RewriteableMsg copy() {
            return  new Response(vodSrc, vodDest, timeoutId, leader, searchDescriptors);
        }

        @Override
        public ByteBuf toByteArray() throws MessageEncodingException {
            ByteBuf buffer = createChannelBufferWithHeader();
            buffer.writeBoolean(leader);
//            UserTypesEncoderFactory.writeListVodNodeDescriptors(buffer, SearchDescriptor.toVodDescriptorList(searchDescriptors));
            ApplicationTypesEncoderFactory.writeSearchDescriptorSet(buffer, new HashSet<SearchDescriptor>(searchDescriptors));
            return buffer;
        }

        @Override
        public byte getOpcode() {
            return MessageFrameDecoder.LEADER_LOOKUP_RESPONSE;
        }
    }

    public static class RequestTimeout extends IndividualTimeout {

        public RequestTimeout(ScheduleTimeout request, int id) {
            super(request, id);
        }
    }
}
