package se.sics.peersearch.msgs;

import java.util.logging.Level;
import java.util.logging.Logger;
import org.jboss.netty.buffer.ChannelBuffer;
import se.sics.gvod.common.msgs.DirectMsgNettyFactory;
import se.sics.gvod.common.msgs.MessageDecodingException;
import se.sics.gvod.net.msgs.DirectMsg;
import se.sics.gvod.net.util.UserTypesDecoderFactory;

public class SearchMsgFactory  {

    public static class Request extends DirectMsgNettyFactory {

        private Request() {
        }

        public static SearchMsg.Request fromBuffer(ChannelBuffer buffer) 
                throws MessageDecodingException {
            return (SearchMsg.Request)
                    new SearchMsgFactory.Request().decode(buffer, true);
        }

        @Override
        protected SearchMsg.Request process(ChannelBuffer buffer) throws MessageDecodingException {
            String query = UserTypesDecoderFactory.readStringLength256(buffer);
            try {
                return new SearchMsg.Request(vodSrc, vodDest,
                        timeoutId, query);
            } catch (SearchMsg.IllegalSearchString ex) {
                Logger.getLogger(SearchMsgFactory.class.getName()).log(Level.SEVERE, null, ex);
            }
            return null;
        }

    }

    public static class Response extends DirectMsgNettyFactory {

        private Response() {
        }

        public static SearchMsg.Response fromBuffer(ChannelBuffer buffer)
                throws MessageDecodingException {
            return (SearchMsg.Response)
                    new SearchMsgFactory.Response().decode(buffer, true);
        }

        @Override
        protected DirectMsg process(ChannelBuffer buffer) throws MessageDecodingException {
            int numResponses = UserTypesDecoderFactory.readIntAsOneByte(buffer);
            int responseNum = UserTypesDecoderFactory.readIntAsOneByte(buffer);
            String results = UserTypesDecoderFactory.readStringLength65536(buffer);
            try {
                return new SearchMsg.Response(vodSrc, vodDest, timeoutId, numResponses, responseNum, results);
            } catch (SearchMsg.IllegalSearchString ex) {
                Logger.getLogger(SearchMsgFactory.class.getName()).log(Level.SEVERE, null, ex);
            }
            return null;
        }
    }
};
