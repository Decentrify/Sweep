package se.sics.peersearch.messages;

import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.VodDescriptor;
import se.sics.gvod.common.msgs.DirectMsgNettyFactory;
import se.sics.gvod.common.msgs.MessageDecodingException;
import se.sics.gvod.common.msgs.RelayMsgNettyFactory;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.msgs.DirectMsg;
import se.sics.gvod.net.msgs.RewriteableMsg;
import se.sics.gvod.net.util.UserTypesDecoderFactory;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 7/2/13
 * Time: 12:20 PM
 */
public class ElectionMessageFactory {
    public static class Request extends  DirectMsgNettyFactory.Request{

        private Request() {
        }

        public static ElectionMessage.Request fromBuffer(ByteBuf buffer) throws MessageDecodingException {
            return (ElectionMessage.Request) new ElectionMessageFactory.Request().decode(buffer, true);
        }

        @Override
        protected DirectMsg process(ByteBuf buffer) throws MessageDecodingException {
            int counter = buffer.readInt();
            VodDescriptor vodDescriptor = UserTypesDecoderFactory.readGVodNodeDescriptor(buffer);
            return new ElectionMessage.Request(vodSrc, vodDest, timeoutId, counter, vodDescriptor);
        }
    }

    public static class Response extends DirectMsgNettyFactory.Response {

        private Response() {
        }

        public static ElectionMessage.Response fromBuffer(ByteBuf buffer) throws MessageDecodingException {
            return (ElectionMessage.Response) new ElectionMessageFactory.Response().decode(buffer, true);
        }


        @Override
        protected DirectMsg process(ByteBuf buffer) throws MessageDecodingException {
            int voteId = buffer.readInt();
            boolean isConvereged = UserTypesDecoderFactory.readBoolean(buffer);
            boolean vote = UserTypesDecoderFactory.readBoolean(buffer);
            VodDescriptor highest = UserTypesDecoderFactory.readGVodNodeDescriptor(buffer);
            return new ElectionMessage.Response(vodSrc, vodDest, timeoutId, voteId, isConvereged, vote, highest);
        }
    }
}
