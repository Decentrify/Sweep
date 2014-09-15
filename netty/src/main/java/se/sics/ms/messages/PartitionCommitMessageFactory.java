package se.sics.ms.messages;

import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.msgs.DirectMsgNettyFactory;
import se.sics.gvod.common.msgs.MessageDecodingException;
import se.sics.gvod.net.util.UserTypesDecoderFactory;
import se.sics.gvod.timer.TimeoutId;

/**
 * Factory class for the PartitionCommitMessage.
 * Created by babbarshaer on 2014-07-21.
 */
public class PartitionCommitMessageFactory {

    public static class Request extends DirectMsgNettyFactory.Request{

        private Request(){

        }

        public static PartitionCommitMessage.Request fromBuffer(ByteBuf buffer) throws MessageDecodingException {
            return (PartitionCommitMessage.Request) new PartitionCommitMessageFactory.Request().decode(buffer);
        }


        @Override
        protected PartitionCommitMessage.Request process(ByteBuf byteBuf) throws MessageDecodingException {
            TimeoutId partitionRequestId = UserTypesDecoderFactory.readTimeoutId(byteBuf);
            PartitionCommitMessage.Request request  = new PartitionCommitMessage.Request(vodSrc,vodDest,timeoutId,partitionRequestId);
            return request;
        }
    }



    public static class Response extends DirectMsgNettyFactory.Response{

        private Response(){

        }

        public static PartitionCommitMessage.Response fromBuffer(ByteBuf buffer) throws MessageDecodingException{
            return (PartitionCommitMessage.Response) new PartitionCommitMessageFactory.Response().decode(buffer);
        }

        @Override
        protected PartitionCommitMessage.Response process(ByteBuf byteBuf) throws MessageDecodingException {

            TimeoutId partitionRequestId = UserTypesDecoderFactory.readTimeoutId(byteBuf);
            PartitionCommitMessage.Response response = new PartitionCommitMessage.Response(vodSrc,vodDest,timeoutId,partitionRequestId);
            return response;
        }
    }

}
