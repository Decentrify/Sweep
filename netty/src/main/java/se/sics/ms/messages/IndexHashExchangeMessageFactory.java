package se.sics.ms.messages;

import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.msgs.DirectMsgNettyFactory;
import se.sics.gvod.common.msgs.MessageDecodingException;
import se.sics.gvod.net.msgs.DirectMsg;
import se.sics.ms.net.ApplicationTypesDecoderFactory;
import se.sics.ms.types.IndexHash;

import java.util.Collection;

public class IndexHashExchangeMessageFactory {
    public static class Request extends DirectMsgNettyFactory.Request {

        private Request() {
        }

        public static IndexHashExchangeMessage.Request fromBuffer(ByteBuf buffer) throws MessageDecodingException {
            return (IndexHashExchangeMessage.Request) new IndexHashExchangeMessageFactory.Request().decode(buffer, true);
        }

        @Override
        protected IndexHashExchangeMessage.Request process(ByteBuf buffer) throws MessageDecodingException {
            long oldestMissingIndexValue = buffer.readLong();
            Long[] existingEntries = ApplicationTypesDecoderFactory.readLongArray(buffer);
            return new IndexHashExchangeMessage.Request(vodSrc, vodDest, timeoutId, oldestMissingIndexValue, existingEntries);
        }

    }

    public static class Response extends DirectMsgNettyFactory.Response {

        private Response() {
        }

        public static IndexHashExchangeMessage.Response fromBuffer(ByteBuf buffer) throws MessageDecodingException {
            return (IndexHashExchangeMessage.Response) new IndexHashExchangeMessageFactory.Response().decode(buffer, true);
        }

        @Override
        protected DirectMsg process(ByteBuf buffer) throws MessageDecodingException {
            Collection<IndexHash> hashes = ApplicationTypesDecoderFactory.readIndexEntryHashCollection(buffer);
            return new IndexHashExchangeMessage.Response(vodSrc, vodDest, timeoutId, hashes);
        }
    }
}
