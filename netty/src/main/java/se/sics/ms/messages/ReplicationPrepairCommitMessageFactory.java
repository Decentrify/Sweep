package se.sics.ms.messages;

import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.msgs.DirectMsgNettyFactory;
import se.sics.gvod.common.msgs.MessageDecodingException;
import se.sics.gvod.net.msgs.DirectMsg;
import se.sics.ms.net.ApplicationTypesDecoderFactory;
import se.sics.ms.types.IndexEntry;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 8/2/13
 * Time: 5:30 PM
 */
public class ReplicationPrepairCommitMessageFactory {
    public static class Request extends DirectMsgNettyFactory.Request {

        private Request() {
        }

        public static ReplicationPrepareCommitMessage.Request fromBuffer(ByteBuf buffer) throws MessageDecodingException {
            return (ReplicationPrepareCommitMessage.Request) new ReplicationPrepairCommitMessageFactory.Request().decode(buffer, true);
        }

        @Override
        protected ReplicationPrepareCommitMessage.Request process(ByteBuf buffer) throws MessageDecodingException {
            IndexEntry entry = ApplicationTypesDecoderFactory.readIndexEntry(buffer);
            return new ReplicationPrepareCommitMessage.Request(vodSrc, vodDest, timeoutId, entry);
        }
    }

    public static class Response extends DirectMsgNettyFactory.Response {

        private Response() {
        }

        public static ReplicationPrepareCommitMessage.Response fromBuffer(ByteBuf buffer)
                throws MessageDecodingException {
            return (ReplicationPrepareCommitMessage.Response) new ReplicationPrepairCommitMessageFactory.Response().decode(buffer, true);
        }

        @Override
        protected DirectMsg process(ByteBuf buffer) throws MessageDecodingException {
            long entryId = buffer.readLong();
            return new ReplicationPrepareCommitMessage.Response(vodSrc, vodDest, timeoutId, entryId);
        }
    }
}

