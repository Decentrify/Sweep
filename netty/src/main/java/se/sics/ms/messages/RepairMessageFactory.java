package se.sics.ms.messages;

import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.msgs.DirectMsgNettyFactory;
import se.sics.gvod.common.msgs.MessageDecodingException;
import se.sics.gvod.net.msgs.DirectMsg;
import se.sics.ms.net.ApplicationTypesDecoderFactory;
import se.sics.ms.types.IndexEntry;

import java.util.Collection;


/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 7/30/13
 * Time: 3:36 PM
 */
public class RepairMessageFactory {
    public static class Request extends DirectMsgNettyFactory.Request {

        private Request() {
        }

        public static RepairMessage.Request fromBuffer(ByteBuf buffer)
                throws MessageDecodingException {
            return (RepairMessage.Request)
                    new RepairMessageFactory.Request().decode(buffer);
        }

        @Override
        protected RepairMessage.Request process(ByteBuf buffer) throws MessageDecodingException {
            Long[] missingIds = ApplicationTypesDecoderFactory.readLongArray(buffer);
            return new RepairMessage.Request(vodSrc, vodDest, timeoutId, missingIds);
        }

    }

    public static class Response extends DirectMsgNettyFactory.Response {

        private Response() {
        }

        public static RepairMessage.Response fromBuffer(ByteBuf buffer)
                throws MessageDecodingException {
            return (RepairMessage.Response)
                    new RepairMessageFactory.Response().decode(buffer);
        }

        @Override
        protected DirectMsg process(ByteBuf buffer) throws MessageDecodingException {
            Collection<IndexEntry> missingEntries = ApplicationTypesDecoderFactory.readIndexEntryCollection(buffer);
            return new RepairMessage.Response(vodSrc, vodDest, timeoutId, missingEntries);
        }
    }
}
