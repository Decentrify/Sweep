package se.sics.peersearch.messages;


import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.msgs.MessageEncodingException;
import se.sics.gvod.common.msgs.DirectMsgNetty;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.msgs.RewriteableMsg;
import se.sics.gvod.net.msgs.RewriteableRetryTimeout;
import se.sics.gvod.net.msgs.ScheduleRetryTimeout;
import se.sics.gvod.net.util.UserTypesEncoderFactory;
import se.sics.gvod.timer.TimeoutId;
import se.sics.gvod.timer.UUID;
import se.sics.peersearch.net.ApplicationTypesEncoderFactory;
import se.sics.peersearch.net.MessageFrameDecoder;
import se.sics.peersearch.types.IndexEntry;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 7/1/13
 * Time: 6:48 PM
 */
public class AddIndexEntryMessage {
    public static class Request extends DirectMsgNetty.Request {
        public static final int MAX_RESULTS_STR_LEN = 1400;

        private final IndexEntry entry;
        private final UUID id;
        private final int numResponses;
        private final int responseNumber;

        public IndexEntry getEntry() {
            return entry;
        }

        public UUID getId() {
            return id;
        }

        public int getNumResponses() {
            return numResponses;
        }

        public int getResponseNumber() {
            return responseNumber;
        }

        public Request(VodAddress source, VodAddress destination, TimeoutId timeoutId, IndexEntry entry, UUID id, int numResponses, int responseNumber) {
            super(source, destination, timeoutId);

            this.entry = entry;
            this.id = id;
            this.numResponses = numResponses;
            this.responseNumber = responseNumber;
        }

        @Override
        public RewriteableMsg copy() {
            return new AddIndexEntryMessage.Request(vodSrc, vodDest, timeoutId, entry, id, numResponses, responseNumber);
        }

        @Override
        public int getSize() {
            return getHeaderSize() + MAX_RESULTS_STR_LEN;
        }

        @Override
        public ByteBuf toByteArray() throws MessageEncodingException {
            ByteBuf buffer = createChannelBufferWithHeader();
            ApplicationTypesEncoderFactory.writeIndexEntry(buffer, entry);
            UserTypesEncoderFactory.writeTimeoutId(buffer, id);
            UserTypesEncoderFactory.writeUnsignedintAsOneByte(buffer, numResponses);
            UserTypesEncoderFactory.writeUnsignedintAsOneByte(buffer, responseNumber);
            return buffer;
        }

        @Override
        public byte getOpcode() {
            return MessageFrameDecoder.ADD_ENTRY_REQUEST;
        }
    }

    public static class Response extends DirectMsgNetty.Response {
        public static final int MAX_RESULTS_STR_LEN = 1400;

        private final IndexEntry entry;
        private final TimeoutId id;
        private final int numResponses;
        private final int responseNumber;

        public TimeoutId getId() {
            return id;
        }

        public IndexEntry getEntry() {
            return entry;
        }

        public int getNumResponses() {
            return numResponses;
        }

        public int getResponseNumber() {
            return responseNumber;
        }

        public Response(VodAddress source, VodAddress destination, TimeoutId timeoutId, IndexEntry entry, TimeoutId id, int numResponses, int responseNumber) {
            super(source, destination, timeoutId);
            this.entry = entry;
            this.id = id;
            this.numResponses = numResponses;
            this.responseNumber = responseNumber;
        }

        @Override
        public RewriteableMsg copy() {
            return new Response(vodSrc, vodDest, timeoutId, entry, id, numResponses, responseNumber);
        }

        @Override
        public int getSize() {
            return getHeaderSize() + MAX_RESULTS_STR_LEN;
        }

        @Override
        public ByteBuf toByteArray() throws MessageEncodingException {
            ByteBuf buffer = createChannelBufferWithHeader();
            ApplicationTypesEncoderFactory.writeIndexEntry(buffer, entry);
            UserTypesEncoderFactory.writeTimeoutId(buffer, id);
            UserTypesEncoderFactory.writeUnsignedintAsOneByte(buffer, numResponses);
            UserTypesEncoderFactory.writeUnsignedintAsOneByte(buffer, responseNumber);
            return buffer;
        }

        @Override
        public byte getOpcode() {
            return MessageFrameDecoder.ADD_ENTRY_RESPONSE;
        }
    }

    public static class RequestTimeout extends RewriteableRetryTimeout {
        public RequestTimeout(ScheduleRetryTimeout st, RewriteableMsg retryMessage) {
            super(st, retryMessage);
        }
    }
}
