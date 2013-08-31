package se.sics.ms.messages;

import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;
import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.msgs.MessageDecodingException;
import se.sics.gvod.common.msgs.DirectMsgNettyFactory;
import se.sics.gvod.net.msgs.DirectMsg;
import se.sics.gvod.net.util.UserTypesDecoderFactory;
import se.sics.gvod.timer.TimeoutId;
import se.sics.ms.exceptions.IllegalSearchString;
import se.sics.ms.net.ApplicationTypesDecoderFactory;
import se.sics.ms.types.IndexEntry;
import se.sics.ms.types.SearchPattern;

public class SearchMessageFactory {

    public static class Request extends DirectMsgNettyFactory.Request {

        private Request() {
        }

        public static SearchMessage.Request fromBuffer(ByteBuf buffer)
                throws MessageDecodingException {
            return (SearchMessage.Request)
                    new SearchMessageFactory.Request().decode(buffer, true);
        }

        @Override
        protected SearchMessage.Request process(ByteBuf buffer) throws MessageDecodingException {
            SearchPattern pattern = ApplicationTypesDecoderFactory.readSearchPattern(buffer);
            TimeoutId searchTimeoutId = UserTypesDecoderFactory.readTimeoutId(buffer);
            int partitionId = buffer.readInt();
            return new SearchMessage.Request(vodSrc, vodDest, timeoutId, searchTimeoutId, pattern, partitionId);
        }

    }

    public static class Response extends DirectMsgNettyFactory.Response {

        private Response() {
        }

        public static SearchMessage.Response fromBuffer(ByteBuf buffer) throws MessageDecodingException {
            return (SearchMessage.Response) new SearchMessageFactory.Response().decode(buffer, true);
        }

        @Override
        protected DirectMsg process(ByteBuf buffer) throws MessageDecodingException {
            int numResponses = UserTypesDecoderFactory.readIntAsOneByte(buffer);
            int responseNum = UserTypesDecoderFactory.readIntAsOneByte(buffer);
            Collection<IndexEntry> results = ApplicationTypesDecoderFactory.readIndexEntryCollection(buffer);
            TimeoutId searchTimeoutId = UserTypesDecoderFactory.readTimeoutId(buffer);
            int partitionId = buffer.readInt();
            try {
                return new SearchMessage.Response(vodSrc, vodDest, timeoutId, searchTimeoutId, numResponses, responseNum, results, partitionId);
            } catch (IllegalSearchString ex) {
                Logger.getLogger(SearchMessageFactory.class.getName()).log(Level.SEVERE, null, ex);
            }
            return null;
        }
    }
};
