package se.sics.ms.messages;

import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.msgs.DirectMsgNettyFactory;
import se.sics.gvod.common.msgs.MessageDecodingException;
import se.sics.gvod.net.msgs.DirectMsg;
import se.sics.gvod.timer.TimeoutId;
import se.sics.ms.net.ApplicationTypesDecoderFactory;
import se.sics.ms.util.PartitionHelper;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

/**
 * Factory class required for the DelayedPartitioningMessage.
 *
 * @author babbarshaer.
 */
public class DelayedPartitioningMessageFactory {



    public static class Request extends DirectMsgNettyFactory.Request{


        public static DelayedPartitioningMessage.Request fromBuffer(ByteBuf buffer) throws MessageDecodingException {
            return (DelayedPartitioningMessage.Request) new DelayedPartitioningMessageFactory.Request().decode(buffer);
        }


        @Override
        protected DelayedPartitioningMessage.Request process(ByteBuf buffer) throws MessageDecodingException {

            List<TimeoutId> partitionRequestIds =  ApplicationTypesDecoderFactory.readPartitionUpdateRequestId(buffer);
            return new DelayedPartitioningMessage.Request(vodSrc,vodDest, timeoutId,partitionRequestIds);
        }
    }

    public static class Response extends DirectMsgNettyFactory.Response{


        public static DelayedPartitioningMessage.Response fromBuffer(ByteBuf buffer) throws MessageDecodingException {
            return (DelayedPartitioningMessage.Response) new DelayedPartitioningMessageFactory.Response().decode(buffer);
        }

        @Override
        protected DelayedPartitioningMessage.Response process(ByteBuf byteBuf) throws MessageDecodingException {

            LinkedList<PartitionHelper.PartitionInfo> partitionUpdates = ApplicationTypesDecoderFactory.readPartitionUpdateSequence(byteBuf);
            return new DelayedPartitioningMessage.Response(vodSrc,vodDest,timeoutId,partitionUpdates);
        }
    }



}
