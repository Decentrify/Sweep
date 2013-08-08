package se.sics.ms.messages;

import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.msgs.DirectMsgNetty;
import se.sics.gvod.common.msgs.MessageEncodingException;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.msgs.RewriteableMsg;
import se.sics.gvod.timer.TimeoutId;
import se.sics.ms.net.ApplicationTypesEncoderFactory;
import se.sics.ms.net.MessageFrameDecoder;
import se.sics.ms.types.IndexEntry;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 7/30/13
 * Time: 3:33 PM
 */
public class RepairMessage {
    public static class Request extends DirectMsgNetty.Request {
        private final Long[] missingIds;

        public Request(VodAddress source, VodAddress destination, TimeoutId timeoutId, Long[] missingIds) {
            super(source, destination, timeoutId);

            if(missingIds == null)
                throw new NullPointerException("missingIds can't be null");

            this.missingIds = missingIds;
        }


        public Long[] getMissingIds() {
            return missingIds;
        }

        @Override
        public int getSize() {
            return getHeaderSize();
        }

        @Override
        public RewriteableMsg copy() {
            return new Request(vodSrc, vodDest, timeoutId, missingIds);
        }

        @Override
        public ByteBuf toByteArray() throws MessageEncodingException {
            ByteBuf buffer = createChannelBufferWithHeader();
            ApplicationTypesEncoderFactory.writeLongArray(buffer, missingIds);
            return buffer;
        }

        @Override
        public byte getOpcode() {
            return MessageFrameDecoder.REPAIR_REQUEST;
        }
    }

    public static class Response extends DirectMsgNetty.Response {
        private final IndexEntry[] missingEntries;

        public Response(VodAddress source, VodAddress destination, TimeoutId timeoutId, IndexEntry[] missingEntries) {
            super(source, destination, timeoutId);
            this.missingEntries = missingEntries;
        }

        public IndexEntry[] getMissingEntries() {
            return missingEntries;
        }

        @Override
        public int getSize() {
            return getHeaderSize();
        }

        @Override
        public RewriteableMsg copy() {
            return new Response(vodSrc, vodDest, timeoutId, missingEntries);
        }

        @Override
        public ByteBuf toByteArray() throws MessageEncodingException {
            ByteBuf buffer = createChannelBufferWithHeader();
            ApplicationTypesEncoderFactory.writeIndexEntryArray(buffer, missingEntries);
            return buffer;
        }

        @Override
        public byte getOpcode() {
            return MessageFrameDecoder.REPAIR_RESPONSE;
        }
    }
}
