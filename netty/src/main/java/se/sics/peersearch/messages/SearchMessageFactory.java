package se.sics.peersearch.messages;

import java.util.logging.Level;
import java.util.logging.Logger;
import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.msgs.MessageDecodingException;
import se.sics.gvod.common.msgs.DirectMsgNettyFactory;
import se.sics.gvod.net.msgs.DirectMsg;
import se.sics.gvod.net.util.UserTypesDecoderFactory;
import se.sics.gvod.timer.UUID;
import se.sics.peersearch.exceptions.IllegalSearchString;
import se.sics.peersearch.net.ApplicationTypesDecoderFactory;
import se.sics.peersearch.types.IndexEntry;
import se.sics.peersearch.types.SearchPattern;

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
            UUID requestId = (UUID)UserTypesDecoderFactory.readTimeoutId(buffer);
            SearchPattern pattern = ApplicationTypesDecoderFactory.readSearchPattern(buffer);
            return new SearchMessage.Request(vodSrc, vodDest,
                    timeoutId, requestId, pattern);
        }

    }

    public static class Response extends DirectMsgNettyFactory.Response {

        private Response() {
        }

        public static SearchMessage.Response fromBuffer(ByteBuf buffer)
                throws MessageDecodingException {
            return (SearchMessage.Response)
                    new SearchMessageFactory.Response().decode(buffer, true);
        }

        @Override
        protected DirectMsg process(ByteBuf buffer) throws MessageDecodingException {
            UUID requestId = (UUID)UserTypesDecoderFactory.readTimeoutId(buffer);
            int numResponses = UserTypesDecoderFactory.readIntAsOneByte(buffer);
            int responseNum = UserTypesDecoderFactory.readIntAsOneByte(buffer);
            IndexEntry[] results = ApplicationTypesDecoderFactory.readIndexEntryArray(buffer);
            try {
                return new SearchMessage.Response(vodSrc, vodDest, timeoutId, requestId, numResponses, responseNum, results);
            } catch (IllegalSearchString ex) {
                Logger.getLogger(SearchMessageFactory.class.getName()).log(Level.SEVERE, null, ex);
            }
            return null;
        }
    }
};
