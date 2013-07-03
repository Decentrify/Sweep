package se.sics.peersearch.messages;

import org.jboss.netty.buffer.ChannelBuffer;
import se.sics.gvod.common.msgs.MessageEncodingException;
import se.sics.gvod.common.msgs.DirectMsgNetty;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.msgs.RewriteableMsg;
import se.sics.gvod.net.msgs.RewriteableRetryTimeout;
import se.sics.gvod.net.msgs.ScheduleRetryTimeout;
import se.sics.gvod.net.util.UserTypesEncoderFactory;
import se.sics.gvod.timer.TimeoutId;
import se.sics.peersearch.net.ApplicationTypesEncoderFactory;
import se.sics.peersearch.net.MessageFrameDecoder;
import se.sics.peersearch.types.IndexEntry;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 7/2/13
 * Time: 12:11 PM
 */
public class GapDetectionMessage {
    public static class Request extends DirectMsgNetty {
        private final long missingEntryId;

        public Request(VodAddress source, VodAddress destination, TimeoutId timeoutId, long missingEntryId) {
            super(source, destination, timeoutId);
            this.missingEntryId = missingEntryId;
        }

        public long getMissingEntryId() {
            return missingEntryId;
        }


        @Override
        public int getSize() {
            return getHeaderSize()+4;
        }

        @Override
        public RewriteableMsg copy() {
            return new Request(vodSrc, vodDest, timeoutId, missingEntryId);
        }

        @Override
        public ChannelBuffer toByteArray() throws MessageEncodingException {
            ChannelBuffer buffer = createChannelBufferWithHeader();
            buffer.writeLong(missingEntryId);
            return buffer;
        }

        @Override
        public byte getOpcode() {
            return MessageFrameDecoder.GAP_DETECTION_REQUEST;
        }
    }

    public static class Response extends DirectMsgNetty {
        public static final int MAX_RESULTS_STR_LEN = 1400;

        private final IndexEntry missingEntry;
        private final int numResponses;
        private final int responseNumber;

        public Response(VodAddress source, VodAddress destination, TimeoutId timeoutId, IndexEntry missingEntry, int numResponses, int responseNumber) {
            super(source, destination, timeoutId);
            this.missingEntry = missingEntry;
            this.numResponses = numResponses;
            this.responseNumber = responseNumber;
        }

        public IndexEntry getMissingEntry() {
            return missingEntry;
        }

        public int getNumResponses() {
            return numResponses;
        }

        public int getResponseNumber() {
            return responseNumber;
        }

        @Override
        public int getSize() {
            return getHeaderSize() + MAX_RESULTS_STR_LEN;
        }

        @Override
        public RewriteableMsg copy() {
            return new Response(vodSrc, vodDest, timeoutId, missingEntry, numResponses, responseNumber);
        }

        @Override
        public ChannelBuffer toByteArray() throws MessageEncodingException {
            ChannelBuffer buffer = createChannelBufferWithHeader();
            ApplicationTypesEncoderFactory.writeIndexEntry(buffer, missingEntry);
            UserTypesEncoderFactory.writeUnsignedintAsOneByte(buffer, numResponses);
            UserTypesEncoderFactory.writeUnsignedintAsOneByte(buffer, responseNumber);

            return buffer;
        }

        @Override
        public byte getOpcode() {
            return MessageFrameDecoder.GAP_DETECTION_RESPONSE;
        }
    }

    public static class RequestTimeout extends RewriteableRetryTimeout {

        public RequestTimeout(ScheduleRetryTimeout st, RewriteableMsg retryMessage) {
            super(st, retryMessage);
        }
    }
}
