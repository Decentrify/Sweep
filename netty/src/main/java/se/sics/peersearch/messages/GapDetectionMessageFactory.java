package se.sics.peersearch.messages;

import io.netty.buffer.ByteBuf;
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
 * Time: 12:14 PM
 */
public class GapDetectionMessageFactory {
    public static class Request extends DirectMsgNettyFactory.Request {

        private Request() {
        }

        public static GapDetectionMessage.Request fromBuffer(ByteBuf buffer)
                throws MessageDecodingException {
            return (GapDetectionMessage.Request)
                    new GapDetectionMessageFactory.Request().decode(buffer, true);
        }

        @Override
        protected GapDetectionMessage.Request process(ByteBuf buffer) throws MessageDecodingException {
            Long id = buffer.readLong();
            return new GapDetectionMessage.Request(vodSrc, vodDest,
                    timeoutId, id);
        }

    }

    public static class Response extends DirectMsgNettyFactory.Response {

        private Response() {
        }

        public static GapDetectionMessage.Response fromBuffer(ByteBuf buffer)
                throws MessageDecodingException {
            return (GapDetectionMessage.Response)
                    new GapDetectionMessageFactory.Response().decode(buffer, true);
        }

        @Override
        protected DirectMsg process(ByteBuf buffer) throws MessageDecodingException {
            IndexEntry entry = ApplicationTypesDecoderFactory.readIndexEntry(buffer);
            int numResponses = UserTypesDecoderFactory.readIntAsOneByte(buffer);
            int responseNum = UserTypesDecoderFactory.readIntAsOneByte(buffer);
            return new GapDetectionMessage.Response(vodSrc, vodDest, timeoutId, entry, numResponses, responseNum);
        }
    }
}
