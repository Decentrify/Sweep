package se.sics.peersearch.messages;

import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.msgs.DirectMsgNetty;
import se.sics.gvod.common.msgs.MessageEncodingException;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.msgs.RewriteableMsg;
import se.sics.gvod.timer.TimeoutId;
import se.sics.peersearch.net.ApplicationTypesEncoderFactory;
import se.sics.peersearch.net.MessageFrameDecoder;
import se.sics.peersearch.types.IndexEntry;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 7/30/13
 * Time: 3:33 PM
 */
public class RepairMessage {
    public static class Request extends DirectMsgNetty.Request {
        private final IndexEntry futureEntry;
        private final Long[] missingIds;

        public Request(VodAddress source, VodAddress destination, TimeoutId timeoutId, IndexEntry futureEntry, Long[] missingIds) {
            super(source, destination, timeoutId);
            this.futureEntry = futureEntry;
            this.missingIds = missingIds;
        }


        public Long[] getMissingIds() {
            return missingIds;
        }

        public IndexEntry getFutureEntry() {
            return futureEntry;
        }

        @Override
        public int getSize() {
            return getHeaderSize();
        }

        @Override
        public RewriteableMsg copy() {
            return new Request(vodSrc, vodDest, timeoutId, futureEntry, missingIds);
        }

        @Override
        public ByteBuf toByteArray() throws MessageEncodingException {
            ByteBuf buffer = createChannelBufferWithHeader();
            ApplicationTypesEncoderFactory.writeIndexEntry(buffer, futureEntry);
            ApplicationTypesEncoderFactory.writeLongArray(buffer, missingIds);
            return buffer;
        }

        @Override
        public byte getOpcode() {
            return MessageFrameDecoder.REPAIR_REQUEST;
        }
    }

    public static class Response extends DirectMsgNetty.Response {
        private final IndexEntry futureEntry;
        private final IndexEntry[] missingEntries;

        public Response(VodAddress source, VodAddress destination, TimeoutId timeoutId, IndexEntry futureEntry, IndexEntry[] missingEntries) {
            super(source, destination, timeoutId);
            this.futureEntry = futureEntry;
            this.missingEntries = missingEntries;
        }

        public IndexEntry[] getMissingEntries() {
            return missingEntries;
        }

        public IndexEntry getFutureEntry() {
            return futureEntry;
        }

        @Override
        public int getSize() {
            return getHeaderSize();
        }

        @Override
        public RewriteableMsg copy() {
            return new Response(vodSrc, vodDest, timeoutId, futureEntry, missingEntries);
        }

        @Override
        public ByteBuf toByteArray() throws MessageEncodingException {
            ByteBuf buffer = createChannelBufferWithHeader();
            ApplicationTypesEncoderFactory.writeIndexEntry(buffer, futureEntry);
            ApplicationTypesEncoderFactory.writeIndexEntryArray(buffer, missingEntries);
            return buffer;
        }

        @Override
        public byte getOpcode() {
            return MessageFrameDecoder.REPAIR_RESPONSE;
        }
    }
}
