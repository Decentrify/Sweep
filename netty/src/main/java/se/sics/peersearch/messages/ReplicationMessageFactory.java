package se.sics.peersearch.messages;

import io.netty.buffer.ByteBuf;
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
 * Date: 7/2/13
 * Time: 9:15 AM
 */
public class ReplicationMessageFactory {
    public static class Request extends DirectMsgNettyFactory {

        private Request() {
        }

        public static ReplicationMessage.Request fromBuffer(ByteBuf buffer)
                throws MessageDecodingException {
            return (ReplicationMessage.Request)
                    new ReplicationMessageFactory.Request().decode(buffer, true);
        }

        @Override
        protected ReplicationMessage.Request process(ByteBuf buffer) throws MessageDecodingException {
            IndexEntry entry = ApplicationTypesDecoderFactory.readIndexEntry(buffer);
            UUID id = (UUID) UserTypesDecoderFactory.readTimeoutId(buffer);
            int numResponses = UserTypesDecoderFactory.readIntAsOneByte(buffer);
            int responseNum = UserTypesDecoderFactory.readIntAsOneByte(buffer);
            return new ReplicationMessage.Request(vodSrc, vodDest,
                    timeoutId,id, entry, numResponses, responseNum);
        }

    }

    public static class Response extends DirectMsgNettyFactory {

        private Response() {
        }

        public static ReplicationMessage.Response fromBuffer(ByteBuf buffer)
                throws MessageDecodingException {
            return (ReplicationMessage.Response)
                    new ReplicationMessageFactory.Response().decode(buffer, true);
        }

        @Override
        protected DirectMsg process(ByteBuf buffer) throws MessageDecodingException {
            UUID id = (UUID) UserTypesDecoderFactory.readTimeoutId(buffer);
            return new ReplicationMessage.Response(vodSrc, vodDest, timeoutId, id);
        }
    }
}
