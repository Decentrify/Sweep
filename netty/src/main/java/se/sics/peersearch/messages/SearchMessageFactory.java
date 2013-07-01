package se.sics.peersearch.messages;

import java.util.logging.Level;
import java.util.logging.Logger;
import org.jboss.netty.buffer.ChannelBuffer;
import se.sics.gvod.common.msgs.DirectMsgNettyFactory;
import se.sics.gvod.common.msgs.MessageDecodingException;
import se.sics.gvod.net.msgs.DirectMsg;
import se.sics.gvod.net.util.UserTypesDecoderFactory;
import se.sics.peersearch.messages.SearchMessage;

public class SearchMessageFactory  {

    public static class Request extends DirectMsgNettyFactory {

        private Request() {
        }

        public static SearchMessage.Request fromBuffer(ChannelBuffer buffer)
                throws MessageDecodingException {
            return (SearchMessage.Request)
                    new SearchMessageFactory.Request().decode(buffer, true);
        }

        @Override
        protected SearchMessage.Request process(ChannelBuffer buffer) throws MessageDecodingException {
            String query = UserTypesDecoderFactory.readStringLength256(buffer);
            try {
                return new SearchMessage.Request(vodSrc, vodDest,
                        timeoutId, query);
            } catch (SearchMessage.IllegalSearchString ex) {
                Logger.getLogger(SearchMessageFactory.class.getName()).log(Level.SEVERE, null, ex);
            }
            return null;
        }

    }

    public static class Response extends DirectMsgNettyFactory {

        private Response() {
        }

        public static SearchMessage.Response fromBuffer(ChannelBuffer buffer)
                throws MessageDecodingException {
            return (SearchMessage.Response)
                    new SearchMessageFactory.Response().decode(buffer, true);
        }

        @Override
        protected DirectMsg process(ChannelBuffer buffer) throws MessageDecodingException {
            int numResponses = UserTypesDecoderFactory.readIntAsOneByte(buffer);
            int responseNum = UserTypesDecoderFactory.readIntAsOneByte(buffer);
            String results = UserTypesDecoderFactory.readStringLength65536(buffer);
            try {
                return new SearchMessage.Response(vodSrc, vodDest, timeoutId, numResponses, responseNum, results);
            } catch (SearchMessage.IllegalSearchString ex) {
                Logger.getLogger(SearchMessageFactory.class.getName()).log(Level.SEVERE, null, ex);
            }
            return null;
        }
    }
};
