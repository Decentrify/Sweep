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
 * Time: 11:56 AM
 */
public class IndexExchangeMessage {
    public static class Request extends DirectMsgNetty {
        public static final int MAX_RESULTS_STR_LEN = 1400;

        private final long oldestMissingIndexValue;
        private final Long[] existingEntries;
        private final int numResponses;
        private final int responseNumber;

        public Request(VodAddress source, VodAddress destination, TimeoutId timeoutId, long oldestMissingIndexValue, Long[] existingEntries, int numResponses, int responseNumber) {
            super(source, destination, timeoutId);
            this.oldestMissingIndexValue = oldestMissingIndexValue;
            this.existingEntries = existingEntries;
            this.numResponses = numResponses;
            this.responseNumber = responseNumber;
        }

        public long getOldestMissingIndexValue() {
            return oldestMissingIndexValue;
        }

        public Long[] getExistingEntries() {
            return existingEntries;
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
            return new Request(vodSrc, vodDest, timeoutId, oldestMissingIndexValue, existingEntries, numResponses, responseNumber);
        }

        @Override
        public ChannelBuffer toByteArray() throws MessageEncodingException {
            ChannelBuffer buffer = createChannelBufferWithHeader();
            buffer.writeLong(oldestMissingIndexValue);
            ApplicationTypesEncoderFactory.writeLongArray(buffer, existingEntries);
            UserTypesEncoderFactory.writeUnsignedintAsOneByte(buffer, numResponses);
            UserTypesEncoderFactory.writeUnsignedintAsOneByte(buffer, responseNumber);
            return buffer;
        }

        @Override
        public byte getOpcode() {
            return MessageFrameDecoder.INDEX_EXCHANGE_REQUEST;
        }
    }

    public static class Response extends DirectMsgNetty {
        public static final int MAX_RESULTS_STR_LEN = 1400;

        private final IndexEntry[] indexEntries;
        private final int numResponses;
        private final int responseNumber;

        public Response(VodAddress source, VodAddress destination, TimeoutId timeoutId, IndexEntry[] indexEntries, int numResponses, int responseNumber) {
            super(source, destination, timeoutId);
            this.indexEntries = indexEntries;
            this.numResponses = numResponses;
            this.responseNumber = responseNumber;
        }

        public IndexEntry[] getIndexEntries() {
            return indexEntries;
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
            return  new Response(vodSrc, vodDest, timeoutId, indexEntries, numResponses, responseNumber);
        }


        @Override
        public ChannelBuffer toByteArray() throws MessageEncodingException {
            ChannelBuffer buffer = createChannelBufferWithHeader();
            ApplicationTypesEncoderFactory.writeIndexEntryArray(buffer, indexEntries);
            UserTypesEncoderFactory.writeUnsignedintAsOneByte(buffer, numResponses);
            UserTypesEncoderFactory.writeUnsignedintAsOneByte(buffer, responseNumber);
            return buffer;
        }

        @Override
        public byte getOpcode() {
            return MessageFrameDecoder.INDEX_EXCHANGE_RESPONSE;
        }
    }

    public static class RequestTimeout extends RewriteableRetryTimeout {

        public RequestTimeout(ScheduleRetryTimeout st, RewriteableMsg retryMessage) {
            super(st, retryMessage);
        }
    }
}
