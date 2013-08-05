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
 * Date: 8/2/13
 * Time: 5:19 PM
 */
public class ReplicationPrepairCommitMessage {
    public static class Request extends DirectMsgNetty.Request {
        private final IndexEntry entry;

        public Request(VodAddress source, VodAddress destination, TimeoutId timeoutId, IndexEntry entry) {
            super(source, destination, timeoutId);
            this.entry = entry;
        }

        public IndexEntry getEntry() {
            return entry;
        }

        @Override
        public int getSize() {
            return getHeaderSize();
        }

        @Override
        public RewriteableMsg copy() {
            return new Request(vodSrc, vodDest, timeoutId, entry);
        }

        @Override
        public ByteBuf toByteArray() throws MessageEncodingException {
            ByteBuf buffer = createChannelBufferWithHeader();
            ApplicationTypesEncoderFactory.writeIndexEntry(buffer, entry);
            return buffer;
        }

        @Override
        public byte getOpcode() {
            return MessageFrameDecoder.PREPAIR_COMMIT_REQUEST;
        }
    }

    public static class Response extends DirectMsgNetty.Response {
        private final long entryId;

        public Response(VodAddress source, VodAddress destination, TimeoutId timeoutId, long entryId) {
            super(source, destination, timeoutId);
            this.entryId = entryId;
        }

        public long getEntryId() {
            return entryId;
        }


        @Override
        public int getSize() {
            return getHeaderSize();
        }

        @Override
        public RewriteableMsg copy() {
            return new Response(vodSrc, vodDest, timeoutId, entryId);
        }

        @Override
        public ByteBuf toByteArray() throws MessageEncodingException {
            ByteBuf buffer = createChannelBufferWithHeader();
            buffer.writeLong(entryId);
            return buffer;
        }

        @Override
        public byte getOpcode() {
            return MessageFrameDecoder.PREPAIR_COMMIT_REQUEST;
        }
    }
}
