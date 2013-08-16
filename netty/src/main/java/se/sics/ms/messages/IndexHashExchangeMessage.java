package se.sics.ms.messages;

import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.msgs.DirectMsgNetty;
import se.sics.gvod.common.msgs.MessageEncodingException;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.msgs.RewriteableMsg;
import se.sics.gvod.timer.ScheduleTimeout;
import se.sics.gvod.timer.TimeoutId;
import se.sics.ms.net.ApplicationTypesEncoderFactory;
import se.sics.ms.net.MessageFrameDecoder;
import se.sics.ms.timeout.IndividualTimeout;
import se.sics.ms.types.IndexHash;

import java.util.Collection;

public class IndexHashExchangeMessage {
    public static class Request extends DirectMsgNetty.Request {

        private final long oldestMissingIndexValue;
        private final Long[] existingEntries;

        public Request(VodAddress source, VodAddress destination, TimeoutId timeoutId, long oldestMissingIndexValue, Long[] existingEntries) {
            super(source, destination, timeoutId);

            if(existingEntries == null)
                throw new NullPointerException("existingEntries can't be null");

            this.oldestMissingIndexValue = oldestMissingIndexValue;
            this.existingEntries = existingEntries;
        }

        public long getOldestMissingIndexValue() {
            return oldestMissingIndexValue;
        }

        public Long[] getExistingEntries() {
            return existingEntries;
        }

        @Override
        public RewriteableMsg copy() {
            return new Request(vodSrc, vodDest, timeoutId, oldestMissingIndexValue, existingEntries);
        }

        @Override
        public ByteBuf toByteArray() throws MessageEncodingException {
            ByteBuf buffer = createChannelBufferWithHeader();
            buffer.writeLong(oldestMissingIndexValue);
            ApplicationTypesEncoderFactory.writeLongArray(buffer, existingEntries);
            return buffer;
        }

        @Override
        public byte getOpcode() {
            return MessageFrameDecoder.INDEX_HASH_EXCHANGE_REQUEST;
        }

        @Override
        public int getSize() {
            return getHeaderSize();
        }
    }

    public static class Response extends DirectMsgNetty.Response {

        private final Collection<IndexHash> hashes;

        public Response(VodAddress source, VodAddress destination, TimeoutId timeoutId, Collection<IndexHash> hashes) {
            super(source, destination, timeoutId);

            if(hashes == null)
                throw new NullPointerException("hashes can't be null");

            this.hashes = hashes;
        }

        public Collection<IndexHash> getHashes() {
            return hashes;
        }

        @Override
        public int getSize() {
            return getHeaderSize();
        }

        @Override
        public RewriteableMsg copy() {
            return  new Response(vodSrc, vodDest, timeoutId, hashes);
        }


        @Override
        public ByteBuf toByteArray() throws MessageEncodingException {
            ByteBuf buffer = createChannelBufferWithHeader();
            ApplicationTypesEncoderFactory.writeIndexEntryHashCollection(buffer, hashes);
            return buffer;
        }

        @Override
        public byte getOpcode() {
            return MessageFrameDecoder.INDEX_HASH_EXCHANGE_RESPONSE;
        }
    }

    public static class RequestTimeout extends IndividualTimeout {

        public RequestTimeout(ScheduleTimeout request, int id) {
            super(request, id);
        }
    }
}
