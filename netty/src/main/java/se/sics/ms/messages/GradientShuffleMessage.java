package se.sics.ms.messages;

import io.netty.buffer.ByteBuf;
import se.sics.ms.types.SearchDescriptor;
import se.sics.gvod.common.msgs.MessageEncodingException;
import se.sics.gvod.common.msgs.DirectMsgNetty;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.msgs.RewriteableMsg;
import se.sics.gvod.net.msgs.RewriteableRetryTimeout;
import se.sics.gvod.net.msgs.ScheduleRetryTimeout;
import se.sics.gvod.timer.ScheduleTimeout;
import se.sics.gvod.timer.TimeoutId;
import se.sics.ms.net.ApplicationTypesEncoderFactory;
import se.sics.ms.net.MessageFrameDecoder;
import se.sics.ms.timeout.IndividualTimeout;

import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 7/2/13
 * Time: 12:23 PM
 */
public class GradientShuffleMessage {
    public static class Request extends DirectMsgNetty.Request {
        public static final int MAX_RESULTS_STR_LEN = 1400;

        private final Set<SearchDescriptor> searchDescriptors;

        public Set<SearchDescriptor> getSearchDescriptors() {
            return searchDescriptors;
        }

        public Request(VodAddress source, VodAddress destination, TimeoutId timeoutId, Set<SearchDescriptor> searchDescriptors) {
            super(source, destination, timeoutId);
            this.searchDescriptors = searchDescriptors;
        }

        @Override
        public int getSize() {
            return getHeaderSize() + MAX_RESULTS_STR_LEN;
        }

        @Override
        public RewriteableMsg copy() {
            return new Request(vodSrc, vodDest, timeoutId, searchDescriptors);
        }

        @Override
        public ByteBuf toByteArray() throws MessageEncodingException {
            ByteBuf buffer = createChannelBufferWithHeader();
            ApplicationTypesEncoderFactory.writeSearchDescriptorSet(buffer, searchDescriptors);
            return buffer;
        }

        @Override
        public byte getOpcode() {
            return MessageFrameDecoder.GRADIENT_SHUFFLE_REQUEST;
        }
    }

    public static class Response extends DirectMsgNetty.Response {
        public static final int MAX_RESULTS_STR_LEN = 1400;

        private final Set<SearchDescriptor> searchDescriptors;

        public Set<SearchDescriptor> getSearchDescriptors() {
            return searchDescriptors;
        }

        public Response(VodAddress source, VodAddress destination, TimeoutId timeoutId, Set<SearchDescriptor> searchDescriptors) {
            super(source, destination, timeoutId);
            this.searchDescriptors = searchDescriptors;
        }

        @Override
        public int getSize() {
            return getHeaderSize()+MAX_RESULTS_STR_LEN;
        }

        @Override
        public RewriteableMsg copy() {
            return new Response(vodSrc, vodDest, timeoutId, searchDescriptors);
        }

        @Override
        public ByteBuf toByteArray() throws MessageEncodingException {
            ByteBuf buffer = createChannelBufferWithHeader();
            ApplicationTypesEncoderFactory.writeSearchDescriptorSet(buffer, searchDescriptors);
            return buffer;
        }

        @Override
        public byte getOpcode() {
            return MessageFrameDecoder.GRADIENT_SHUFFLE_RESPONSE;
        }
    }

    public static class RequestTimeout extends IndividualTimeout {

        public RequestTimeout(ScheduleTimeout request, int id) {
            super(request, id);
        }
    }
}
