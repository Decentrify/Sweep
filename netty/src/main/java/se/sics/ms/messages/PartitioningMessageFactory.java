package se.sics.ms.messages;

import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.msgs.DirectMsgNettyFactory;
import se.sics.gvod.common.msgs.MessageDecodingException;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.util.UserTypesDecoderFactory;
import se.sics.gvod.timer.TimeoutId;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 8/20/13
 * Time: 11:15 AM
 */
public class PartitioningMessageFactory extends DirectMsgNettyFactory.Oneway {

    private PartitioningMessageFactory() {
    }

    public static PartitioningMessage fromBuffer(ByteBuf buffer) throws MessageDecodingException {
        return (PartitioningMessage) new PartitioningMessageFactory().decode(buffer);
    }

    @Override
    protected PartitioningMessage process(ByteBuf buffer) throws MessageDecodingException {
        long middleEntryId = buffer.readLong();
        TimeoutId requestId = UserTypesDecoderFactory.readTimeoutId(buffer);
        int partitionsNumber = buffer.readInt();
        return new PartitioningMessage(vodSrc, vodDest, requestId, middleEntryId,
                VodAddress.PartitioningType.values()[partitionsNumber]);
    }
}
