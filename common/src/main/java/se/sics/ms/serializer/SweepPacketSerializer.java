package se.sics.ms.serializer;

import io.netty.buffer.ByteBuf;
import se.sics.ms.types.SweepAggregatedPacket;
import se.sics.p2ptoolbox.serialization.SerializationContext;
import se.sics.p2ptoolbox.serialization.Serializer;

/**
 * Serializer for the Aggregated Packet From Sweep.
 * Created by babbar on 2015-03-17.
 */
public class SweepPacketSerializer implements Serializer<SweepAggregatedPacket> {

    @Override
    public ByteBuf encode(SerializationContext serializationContext, ByteBuf buffer, SweepAggregatedPacket obj) throws SerializerException, SerializationContext.MissingException {

        buffer.writeInt(obj.getNodeId());
        buffer.writeInt(obj.getPartitionId());
        buffer.writeInt(obj.getPartitionDepth());
        buffer.writeLong(obj.getNumberOfEntries());

        return buffer;
    }


    @Override
    public SweepAggregatedPacket decode(SerializationContext serializationContext, ByteBuf buffer) throws SerializerException, SerializationContext.MissingException {

        int nodeId = buffer.readInt();
        int partitionId = buffer.readInt();
        int partitionDepth = buffer.readInt();
        long entries = buffer.readLong();

        return new SweepAggregatedPacket(nodeId, partitionId, partitionDepth, entries);
    }


    @Override
    public int getSize(SerializationContext serializationContext, SweepAggregatedPacket sweepAggregatedPacket) throws SerializerException, SerializationContext.MissingException {

        int size = 0;
        size += 3* (Integer.SIZE/8);
        size += (Long.SIZE/8);

        return size;
    }
}
