package se.sics.ms.messages;

import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.msgs.MessageDecodingException;
import se.sics.gvod.common.msgs.DirectMsgNettyFactory;
import se.sics.gvod.net.msgs.DirectMsg;
import se.sics.ms.net.ApplicationTypesDecoderFactory;
import se.sics.ms.types.IndexEntry;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 7/1/13
 * Time: 7:45 PM
 */
public class AddIndexEntryMessageFactory {
    public static class Request extends DirectMsgNettyFactory.Request {

        private Request() {
        }

        public static AddIndexEntryMessage.Request fromBuffer(ByteBuf buffer) throws MessageDecodingException {
            return (AddIndexEntryMessage.Request) new AddIndexEntryMessageFactory.Request().decode(buffer);
        }

        @Override
        protected AddIndexEntryMessage.Request process(ByteBuf buffer) throws MessageDecodingException {
            IndexEntry entry = ApplicationTypesDecoderFactory.readIndexEntry(buffer);
            return new AddIndexEntryMessage.Request(vodSrc, vodDest, timeoutId, entry);
        }
    }

    public static class Response extends DirectMsgNettyFactory.Response {

        private Response() {
        }

        public static AddIndexEntryMessage.Response fromBuffer(ByteBuf buffer)
                throws MessageDecodingException {
            return (AddIndexEntryMessage.Response) new AddIndexEntryMessageFactory.Response().decode(buffer);
        }

        @Override
        protected DirectMsg process(ByteBuf buffer) throws MessageDecodingException {
            return new AddIndexEntryMessage.Response(vodSrc, vodDest, timeoutId);
        }
    }
}
