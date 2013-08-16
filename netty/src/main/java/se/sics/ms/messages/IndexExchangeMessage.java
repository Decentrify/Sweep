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
import se.sics.ms.types.Id;
import se.sics.ms.types.IndexEntry;

import java.util.Collection;

public class IndexExchangeMessage {
    public static class Request extends DirectMsgNetty.Request {

        private final Collection<Id> ids;

        public Request(VodAddress source, VodAddress destination, TimeoutId timeoutId, Collection<Id> ids) {
            super(source, destination, timeoutId);

            if(ids == null)
                throw new NullPointerException("ids can't be null");

            this.ids = ids;
        }

        public Collection<Id> getIds() {
            return ids;
        }

        @Override
        public int getSize() {
            return getHeaderSize();
        }

        @Override
        public RewriteableMsg copy() {
            return new Request(vodSrc, vodDest, timeoutId, ids);
        }

        @Override
        public ByteBuf toByteArray() throws MessageEncodingException {
            ByteBuf buffer = createChannelBufferWithHeader();
            ApplicationTypesEncoderFactory.writeIdCollection(buffer, ids);
            return buffer;
        }

        @Override
        public byte getOpcode() {
            return MessageFrameDecoder.INDEX_EXCHANGE_REQUEST;
        }
    }

    public static class Response extends DirectMsgNetty.Response {
        public static final int MAX_RESULTS_STR_LEN = 1400;

        private final Collection<IndexEntry> indexEntries;
        private final int numResponses;
        private final int responseNumber;

        public Response(VodAddress source, VodAddress destination, TimeoutId timeoutId, Collection<IndexEntry> indexEntries, int numResponses, int responseNumber) {
            super(source, destination, timeoutId);

            if(indexEntries == null)
                throw new NullPointerException("indexEntries can't be null");

            this.indexEntries = indexEntries;
            this.numResponses = numResponses;
            this.responseNumber = responseNumber;
        }

        public Collection<IndexEntry> getIndexEntries() {
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
        public ByteBuf toByteArray() throws MessageEncodingException {
            ByteBuf buffer = createChannelBufferWithHeader();
            ApplicationTypesEncoderFactory.writeIndexEntryCollection(buffer, indexEntries);
            UserTypesEncoderFactory.writeUnsignedintAsOneByte(buffer, numResponses);
            UserTypesEncoderFactory.writeUnsignedintAsOneByte(buffer, responseNumber);
            return buffer;
        }

        @Override
        public byte getOpcode() {
            return MessageFrameDecoder.INDEX_EXCHANGE_RESPONSE;
        }
    }

    public static class RequestTimeout extends IndividualTimeout {

        public RequestTimeout(ScheduleTimeout request, int id) {
            super(request, id);
        }
    }
}
