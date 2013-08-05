package se.sics.peersearch.messages;

import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.msgs.DirectMsgNettyFactory;
import se.sics.gvod.common.msgs.MessageDecodingException;
import se.sics.gvod.net.msgs.DirectMsg;
import se.sics.peersearch.net.ApplicationTypesDecoderFactory;
import se.sics.peersearch.types.IndexEntry;

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

        public static ReplicationPrepairCommitMessage.Request fromBuffer(ByteBuf buffer) throws MessageDecodingException {
            return (ReplicationPrepairCommitMessage.Request) new ReplicationPrepairCommitMessageFactory.Request().decode(buffer, true);
        }

        @Override
        protected ReplicationPrepairCommitMessage.Request process(ByteBuf buffer) throws MessageDecodingException {
            IndexEntry entry = ApplicationTypesDecoderFactory.readIndexEntry(buffer);
            return new ReplicationPrepairCommitMessage.Request(vodSrc, vodDest, timeoutId, entry);
        }
    }

    public static class Response extends DirectMsgNettyFactory.Response {

        private Response() {
        }

        public static ReplicationPrepairCommitMessage.Response fromBuffer(ByteBuf buffer)
                throws MessageDecodingException {
            return (ReplicationPrepairCommitMessage.Response) new ReplicationPrepairCommitMessageFactory.Response().decode(buffer, true);
        }

        @Override
        protected DirectMsg process(ByteBuf buffer) throws MessageDecodingException {
            long entryId = buffer.readLong();
            return new ReplicationPrepairCommitMessage.Response(vodSrc, vodDest, timeoutId, entryId);
        }
    }
}

