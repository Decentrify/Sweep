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
 * @author Steffen Grohsschmiedt
 */
public class AddIndexEntryMessage {

    public static class Request extends DirectMsgNetty.Request {
        public static final int MAX_RESULTS_STR_LEN = 1400;

        private final IndexEntry entry;

        public IndexEntry getEntry() {
            return entry;
        }

        public Request(VodAddress source, VodAddress destination, TimeoutId timeoutId, IndexEntry entry) {
            super(source, destination, timeoutId);

            if(entry == null)
                throw new NullPointerException("entry can't be null");

            this.entry = entry;
        }

        @Override
        public RewriteableMsg copy() {
            return new AddIndexEntryMessage.Request(vodSrc, vodDest, timeoutId, entry);
        }

        @Override
        public int getSize() {
            return getHeaderSize() + MAX_RESULTS_STR_LEN;
        }

        @Override
        public ByteBuf toByteArray() throws MessageEncodingException {
            ByteBuf buffer = createChannelBufferWithHeader();
            ApplicationTypesEncoderFactory.writeIndexEntry(buffer, entry);
            return buffer;
        }

        @Override
        public byte getOpcode() {
            return MessageFrameDecoder.ADD_ENTRY_REQUEST;
        }
    }

    public static class Response extends DirectMsgNetty.Response {
        public static final int MAX_RESULTS_STR_LEN = 1400;

        public Response(VodAddress source, VodAddress destination, TimeoutId timeoutId) {
            super(source, destination, timeoutId);
        }

        @Override
        public RewriteableMsg copy() {
            return new Response(vodSrc, vodDest, timeoutId);
        }

        @Override
        public int getSize() {
            return getHeaderSize() + MAX_RESULTS_STR_LEN;
        }

        @Override
        public ByteBuf toByteArray() throws MessageEncodingException {
            ByteBuf buffer = createChannelBufferWithHeader();
            return buffer;
        }

        @Override
        public byte getOpcode() {
            return MessageFrameDecoder.ADD_ENTRY_RESPONSE;
        }
    }
}
