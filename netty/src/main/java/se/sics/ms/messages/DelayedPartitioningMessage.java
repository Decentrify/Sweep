package se.sics.ms.messages;

import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.msgs.DirectMsgNetty;
import se.sics.gvod.common.msgs.MessageEncodingException;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.msgs.RewriteableMsg;
import se.sics.ms.net.ApplicationTypesEncoderFactory;
import se.sics.ms.net.MessageFrameDecoder;
import se.sics.ms.util.PartitionHelper;

import java.util.LinkedList;

/**
 * Message used to inform the nodes about the partition update history.
 * @author babbarshaer.
 */
public class DelayedPartitioningMessage extends DirectMsgNetty.Oneway{

    LinkedList<PartitionHelper.PartitionInfo> partitionHistory;

    public DelayedPartitioningMessage(VodAddress source, VodAddress destination , LinkedList<PartitionHelper.PartitionInfo> partitionHistory) {
        super(source, destination);
        this.partitionHistory = partitionHistory;
    }

    @Override
    public int getSize() {
        return getHeaderSize();
    }

    @Override
    public RewriteableMsg copy() {
        return new DelayedPartitioningMessage(vodSrc,vodDest, partitionHistory);
    }

    @Override
    public ByteBuf toByteArray() throws MessageEncodingException {
        ByteBuf buffer = createChannelBufferWithHeader();
        ApplicationTypesEncoderFactory.writeDelayedPartitionInfo(buffer,partitionHistory);
        return buffer;
    }

    @Override
    public byte getOpcode() {
        return MessageFrameDecoder.DELAYED_PARTITIONING_MESSAGE;
    }

    public LinkedList<PartitionHelper.PartitionInfo> getPartitionHistory(){
        return this.partitionHistory;
    }
}
