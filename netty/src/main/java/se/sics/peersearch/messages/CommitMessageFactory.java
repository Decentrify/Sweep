package se.sics.peersearch.messages;

import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.msgs.DirectMsgNettyFactory;
import se.sics.gvod.common.msgs.MessageDecodingException;
import se.sics.gvod.net.msgs.DirectMsg;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 8/2/13
 * Time: 5:41 PM
 */
public class CommitMessageFactory {
    public static class Request extends DirectMsgNettyFactory.Request {

        private Request() {
        }

        public static ReplicationCommitMessage.Request fromBuffer(ByteBuf buffer) throws MessageDecodingException {
            return (ReplicationCommitMessage.Request) new CommitMessageFactory.Request().decode(buffer, true);
        }

        @Override
        protected ReplicationCommitMessage.Request process(ByteBuf buffer) throws MessageDecodingException {
            long entryId = buffer.readLong();
            return new ReplicationCommitMessage.Request(vodSrc, vodDest, timeoutId, entryId);
        }
    }

    public static class Response extends DirectMsgNettyFactory.Response {

        private Response() {
        }

        public static ReplicationCommitMessage.Response fromBuffer(ByteBuf buffer)
                throws MessageDecodingException {
            return (ReplicationCommitMessage.Response) new CommitMessageFactory.Response().decode(buffer, true);
        }

        @Override
        protected DirectMsg process(ByteBuf buffer) throws MessageDecodingException {
            long entryId = buffer.readLong();
            return new ReplicationCommitMessage.Response(vodSrc, vodDest, timeoutId, entryId);
        }
    }
}