package se.sics.peersearch.messages;

import org.jboss.netty.buffer.ChannelBuffer;
import se.sics.gvod.common.msgs.MessageDecodingException;
import se.sics.gvod.common.msgs.DirectMsgNettyFactory;
import se.sics.gvod.net.msgs.DirectMsg;
import se.sics.gvod.net.util.UserTypesDecoderFactory;
import se.sics.peersearch.net.ApplicationTypesDecoderFactory;
import se.sics.peersearch.types.IndexEntry;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 7/2/13
 * Time: 12:04 PM
 */
public class IndexExchangeMessageFactory {
    public static class Request extends DirectMsgNettyFactory {

        private Request() {
        }

        public static IndexExchangeMessage.Request fromBuffer(ChannelBuffer buffer)
                throws MessageDecodingException {
            return (IndexExchangeMessage.Request)
                    new IndexExchangeMessageFactory.Request().decode(buffer, true);
        }

        @Override
        protected IndexExchangeMessage.Request process(ChannelBuffer buffer) throws MessageDecodingException {
            long oldestMissingIndexValue = buffer.readLong();
            Long[] existingEntries = ApplicationTypesDecoderFactory.readLongArray(buffer);
            int numResponses = UserTypesDecoderFactory.readIntAsOneByte(buffer);
            int responseNum = UserTypesDecoderFactory.readIntAsOneByte(buffer);
            return new IndexExchangeMessage.Request(vodSrc, vodDest,
                    timeoutId, oldestMissingIndexValue, existingEntries, numResponses, responseNum);
        }

    }

    public static class Response extends DirectMsgNettyFactory {

        private Response() {
        }

        public static IndexExchangeMessage.Response fromBuffer(ChannelBuffer buffer)
                throws MessageDecodingException {
            return (IndexExchangeMessage.Response)
                    new IndexExchangeMessageFactory.Response().decode(buffer, true);
        }

        @Override
        protected DirectMsg process(ChannelBuffer buffer) throws MessageDecodingException {
            IndexEntry[] items = ApplicationTypesDecoderFactory.readIndexEntryArray(buffer);
            int numResponses = UserTypesDecoderFactory.readIntAsOneByte(buffer);
            int responseNum = UserTypesDecoderFactory.readIntAsOneByte(buffer);
            return new IndexExchangeMessage.Response(vodSrc, vodDest, timeoutId, items, numResponses, responseNum);
        }
    }
}
