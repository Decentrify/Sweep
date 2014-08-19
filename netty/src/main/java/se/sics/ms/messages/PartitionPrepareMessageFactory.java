package se.sics.ms.messages;

import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.msgs.DirectMsgNettyFactory;
import se.sics.gvod.common.msgs.MessageDecodingException;
import se.sics.gvod.net.msgs.DirectMsg;
import se.sics.gvod.net.util.UserTypesDecoderFactory;
import se.sics.gvod.timer.TimeoutId;
import se.sics.ms.net.ApplicationTypesDecoderFactory;
import se.sics.ms.util.PartitionHelper;

/**
 * Factory class for the Partition Prepare Message.
 * Created by babbarshaer on 2014-07-21.
 */
public class PartitionPrepareMessageFactory {

    public static class Request extends DirectMsgNettyFactory.Request{

        private Request(){

        }

        public static PartitionPrepareMessage.Request fromBuffer(ByteBuf buffer) throws MessageDecodingException {
            return (PartitionPrepareMessage.Request) new PartitionPrepareMessageFactory.Request().decode(buffer);
        }


        @Override
        protected PartitionPrepareMessage.Request process(ByteBuf byteBuf) throws MessageDecodingException {
            PartitionHelper.PartitionInfo partitionInfo = ApplicationTypesDecoderFactory.readPartitionUpdate(byteBuf);
            PartitionPrepareMessage.Request request  = new PartitionPrepareMessage.Request(vodSrc,vodDest,timeoutId,partitionInfo);
            return request;
        }
    }



    public static class Response extends DirectMsgNettyFactory.Response{

        private Response(){

        }

        public static PartitionPrepareMessage.Response fromBuffer(ByteBuf buffer) throws MessageDecodingException{
            return (PartitionPrepareMessage.Response) new PartitionPrepareMessageFactory.Response().decode(buffer);
        }

        @Override
        protected PartitionPrepareMessage.Response process(ByteBuf byteBuf) throws MessageDecodingException {

            TimeoutId partitionRequestId = UserTypesDecoderFactory.readTimeoutId(byteBuf);
            PartitionPrepareMessage.Response response = new PartitionPrepareMessage.Response(vodSrc,vodDest,timeoutId,partitionRequestId);
            return response;
        }
    }

}
