package se.sics.ms.messages;

import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.msgs.DirectMsgNettyFactory;
import se.sics.gvod.common.msgs.MessageDecodingException;
import se.sics.gvod.net.msgs.DirectMsg;
import se.sics.ms.net.ApplicationTypesDecoderFactory;
import se.sics.ms.util.PartitionHelper;

import java.nio.ByteBuffer;
import java.util.LinkedList;

/**
 * Factory class required for the DelayedPartitioningMessage.
 *
 * @author babbarshaer.
 */
public class DelayedPartitioningMessageFactory extends DirectMsgNettyFactory.Oneway {


    private DelayedPartitioningMessageFactory(){

    }

    public static DelayedPartitioningMessage fromBuffer(ByteBuf buffer) throws MessageDecodingException {
        return (DelayedPartitioningMessage) new DelayedPartitioningMessageFactory().decode(buffer);
    }


    @Override
    protected DelayedPartitioningMessage process(ByteBuf buffer) throws MessageDecodingException {

        LinkedList<PartitionHelper.PartitionInfo> partitionUpdates =  ApplicationTypesDecoderFactory.readPartitionUpdateSequence(buffer);
        return new DelayedPartitioningMessage(vodSrc,vodDest, partitionUpdates);
    }
}
