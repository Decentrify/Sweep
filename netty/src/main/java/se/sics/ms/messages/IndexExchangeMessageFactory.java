package se.sics.ms.messages;

import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.msgs.DirectMsgNettyFactory;
import se.sics.gvod.common.msgs.MessageDecodingException;
import se.sics.gvod.net.msgs.DirectMsg;
import se.sics.gvod.net.util.UserTypesDecoderFactory;
import se.sics.ms.net.ApplicationTypesDecoderFactory;
import se.sics.ms.types.Id;
import se.sics.ms.types.IndexEntry;

import java.util.Collection;

public class IndexExchangeMessageFactory {
    public static class Request extends DirectMsgNettyFactory.Request {

        private Request() {
        }

        public static IndexExchangeMessage.Request fromBuffer(ByteBuf buffer)
                throws MessageDecodingException {
            return (IndexExchangeMessage.Request)
                    new IndexExchangeMessageFactory.Request().decode(buffer);
        }

        @Override
        protected IndexExchangeMessage.Request process(ByteBuf buffer) throws MessageDecodingException {
            Collection<Id> ids = ApplicationTypesDecoderFactory.readIdCollection(buffer);
            return new IndexExchangeMessage.Request(vodSrc, vodDest, timeoutId, ids);
        }

    }

    public static class Response extends DirectMsgNettyFactory.Response {

        private Response() {
        }

        public static IndexExchangeMessage.Response fromBuffer(ByteBuf buffer)
                throws MessageDecodingException {
            return (IndexExchangeMessage.Response)
                    new IndexExchangeMessageFactory.Response().decode(buffer);
        }

        @Override
        protected DirectMsg process(ByteBuf buffer) throws MessageDecodingException {
            Collection<IndexEntry> items = ApplicationTypesDecoderFactory.readIndexEntryCollection(buffer);
            int numResponses = UserTypesDecoderFactory.readIntAsOneByte(buffer);
            int responseNum = UserTypesDecoderFactory.readIntAsOneByte(buffer);
            return new IndexExchangeMessage.Response(vodSrc, vodDest, timeoutId, items, numResponses, responseNum);
        }
    }
}
