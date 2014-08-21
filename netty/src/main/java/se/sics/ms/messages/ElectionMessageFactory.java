package se.sics.ms.messages;

import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.msgs.DirectMsgNettyFactory;
import se.sics.gvod.common.msgs.MessageDecodingException;
import se.sics.gvod.net.msgs.DirectMsg;
import se.sics.gvod.net.util.UserTypesDecoderFactory;
import se.sics.ms.types.SearchDescriptor;

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
            return (ElectionMessage.Request) new ElectionMessageFactory.Request().decode(buffer);
        }

        @Override
        protected DirectMsg process(ByteBuf buffer) throws MessageDecodingException {
            int counter = buffer.readInt();
            SearchDescriptor searchDescriptor = new SearchDescriptor(UserTypesDecoderFactory.readVodNodeDescriptor(buffer));
            return new ElectionMessage.Request(vodSrc, vodDest, timeoutId, counter, searchDescriptor);
        }
    }

    public static class Response extends DirectMsgNettyFactory.Response {

        private Response() {
        }

        public static ElectionMessage.Response fromBuffer(ByteBuf buffer) throws MessageDecodingException {
            return (ElectionMessage.Response) new ElectionMessageFactory.Response().decode(buffer);
        }


        @Override
        protected DirectMsg process(ByteBuf buffer) throws MessageDecodingException {
            int voteId = buffer.readInt();
            boolean isConvereged = UserTypesDecoderFactory.readBoolean(buffer);
            boolean vote = UserTypesDecoderFactory.readBoolean(buffer);
            SearchDescriptor highest = new SearchDescriptor(UserTypesDecoderFactory.readVodNodeDescriptor(buffer));
            return new ElectionMessage.Response(vodSrc, vodDest, timeoutId, voteId, isConvereged, vote, highest);
        }
    }
}
