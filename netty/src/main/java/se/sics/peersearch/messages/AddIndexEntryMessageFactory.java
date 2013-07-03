package se.sics.peersearch.messages;

import org.jboss.netty.buffer.ChannelBuffer;
import se.sics.gvod.common.msgs.MessageDecodingException;
import se.sics.gvod.common.msgs.DirectMsgNettyFactory;
import se.sics.gvod.net.msgs.DirectMsg;
import se.sics.gvod.net.util.UserTypesDecoderFactory;
import se.sics.gvod.timer.UUID;
import se.sics.peersearch.net.ApplicationTypesDecoderFactory;
import se.sics.peersearch.types.IndexEntry;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 7/1/13
 * Time: 7:45 PM
 */
public class AddIndexEntryMessageFactory {
    public static class Request extends DirectMsgNettyFactory {

        private Request() {
        }

        public static AddIndexEntryMessage.Request fromBuffer(ChannelBuffer buffer)
                throws MessageDecodingException {
            return (AddIndexEntryMessage.Request)
                    new AddIndexEntryMessageFactory.Request().decode(buffer, true);
        }

        @Override
        protected AddIndexEntryMessage.Request process(ChannelBuffer buffer) throws MessageDecodingException {
            IndexEntry entry = ApplicationTypesDecoderFactory.readIndexEntry(buffer);
            UUID id = (UUID) UserTypesDecoderFactory.readTimeoutId(buffer);
            int numResponses = UserTypesDecoderFactory.readIntAsOneByte(buffer);
            int responseNum = UserTypesDecoderFactory.readIntAsOneByte(buffer);
            return new AddIndexEntryMessage.Request(vodSrc, vodDest,
                    timeoutId, entry, id, numResponses, responseNum);
        }

    }

    public static class Response extends DirectMsgNettyFactory {

        private Response() {
        }

        public static AddIndexEntryMessage.Response fromBuffer(ChannelBuffer buffer)
                throws MessageDecodingException {
            return (AddIndexEntryMessage.Response)
                    new AddIndexEntryMessageFactory.Response().decode(buffer, true);
        }

        @Override
        protected DirectMsg process(ChannelBuffer buffer) throws MessageDecodingException {
            IndexEntry entry = ApplicationTypesDecoderFactory.readIndexEntry(buffer);
            UUID id = (UUID) UserTypesDecoderFactory.readTimeoutId(buffer);
            int numResponses = UserTypesDecoderFactory.readIntAsOneByte(buffer);
            int responseNum = UserTypesDecoderFactory.readIntAsOneByte(buffer);
            return new AddIndexEntryMessage.Response(vodSrc, vodDest, timeoutId, entry, id, numResponses, responseNum);
        }
    }
}
