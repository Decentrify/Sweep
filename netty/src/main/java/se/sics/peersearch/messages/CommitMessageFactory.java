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

        public static CommitMessage.Request fromBuffer(ByteBuf buffer) throws MessageDecodingException {
            return (CommitMessage.Request) new CommitMessageFactory.Request().decode(buffer, true);
        }

        @Override
        protected CommitMessage.Request process(ByteBuf buffer) throws MessageDecodingException {
            long entryId = buffer.readLong();
            return new CommitMessage.Request(vodSrc, vodDest, timeoutId, entryId);
        }
    }

    public static class Response extends DirectMsgNettyFactory.Response {

        private Response() {
        }

        public static CommitMessage.Response fromBuffer(ByteBuf buffer)
                throws MessageDecodingException {
            return (CommitMessage.Response) new CommitMessageFactory.Response().decode(buffer, true);
        }

        @Override
        protected DirectMsg process(ByteBuf buffer) throws MessageDecodingException {
            long entryId = buffer.readLong();
            return new CommitMessage.Response(vodSrc, vodDest, timeoutId, entryId);
        }
    }
}