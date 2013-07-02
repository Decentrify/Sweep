package se.sics.peersearch.messages;

import org.jboss.netty.buffer.ChannelBuffer;
import se.sics.gvod.common.msgs.MessageEncodingException;
import se.sics.gvod.common.msgs.VodMsgNetty;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.msgs.RewriteableMsg;
import se.sics.gvod.net.msgs.RewriteableRetryTimeout;
import se.sics.gvod.net.msgs.ScheduleRetryTimeout;
import se.sics.gvod.net.util.UserTypesEncoderFactory;
import se.sics.gvod.timer.TimeoutId;
import se.sics.gvod.timer.UUID;
import se.sics.peersearch.net.ApplicationTypesDecoderFactory;
import se.sics.peersearch.net.ApplicationTypesEncoderFactory;
import se.sics.peersearch.net.MessageFrameDecoder;
import se.sics.peersearch.types.IndexEntry;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 7/2/13
 * Time: 9:11 AM
 */
public class ReplicationMessage {
    public static class Request extends VodMsgNetty {
        public static final int MAX_RESULTS_STR_LEN = 1400;

        private final UUID id;
        private final IndexEntry indexEntry;
        private final int numResponses;
        private final int responseNumber;

        public Request(VodAddress source, VodAddress destination, TimeoutId timeoutId, UUID id, IndexEntry indexEntry, int numResponses, int responseNumber) {
            super(source, destination, timeoutId);
            this.id = id;
            this.indexEntry = indexEntry;
            this.numResponses = numResponses;
            this.responseNumber = responseNumber;
        }

        public UUID getId() {
            return id;
        }

        public IndexEntry getIndexEntry() {
            return indexEntry;
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
            return new ReplicationMessage.Request(vodSrc, vodDest, timeoutId, id, indexEntry, numResponses, responseNumber);
        }

        @Override
        public ChannelBuffer toByteArray() throws MessageEncodingException {
            ChannelBuffer buffer = createChannelBufferWithHeader();
            ApplicationTypesEncoderFactory.writeIndexEntry(buffer, indexEntry);
            UserTypesEncoderFactory.writeTimeoutId(buffer, id);
            UserTypesEncoderFactory.writeUnsignedintAsOneByte(buffer, numResponses);
            UserTypesEncoderFactory.writeUnsignedintAsOneByte(buffer, responseNumber);
            return buffer;
        }

        @Override
        public byte getOpcode() {
            return MessageFrameDecoder.REPLICATION_REQUEST;
        }
    }

    public static class Response extends VodMsgNetty {
        private final UUID id;

        public Response(VodAddress source, VodAddress destination, TimeoutId timeoutId, UUID id) {
            super(source, destination, timeoutId);
            this.id = id;
        }

        public UUID getId() {
            return id;
        }

        @Override
        public int getSize() {
            return getHeaderSize() + 4;
        }

        @Override
        public RewriteableMsg copy() {
            return new Response(vodSrc, vodDest, timeoutId, id);
        }

        @Override
        public ChannelBuffer toByteArray() throws MessageEncodingException {
            ChannelBuffer buffer = createChannelBufferWithHeader();
            UserTypesEncoderFactory.writeTimeoutId(buffer, id);
            return buffer;
        }

        @Override
        public byte getOpcode() {
            return MessageFrameDecoder.REPLICATION_RESPONSE;
        }
    }

    public static class RequestTimeout extends RewriteableRetryTimeout {

        public RequestTimeout(ScheduleRetryTimeout st, RewriteableMsg retryMessage) {
            super(st, retryMessage);
        }
    }
}
