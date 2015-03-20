package se.sics.ms.aggregator;

import io.netty.buffer.ByteBuf;
import se.sics.p2ptoolbox.serialization.SerializationContext;
import se.sics.p2ptoolbox.serialization.Serializer;

/**
 * Created by babbarshaer on 2015-03-20.
 */
public class SearchComponentUpdateSerializer implements Serializer<SearchComponentUpdate>{


    @Override
    public ByteBuf encode(SerializationContext serializationContext, ByteBuf buffer, SearchComponentUpdate obj) throws SerializerException, SerializationContext.MissingException {
        
        buffer.writeInt(obj.getNodeId());
        buffer.writeInt(obj.getPartitionId());
        buffer.writeInt(obj.getPartitionDepth());
        buffer.writeLong(obj.getNumberOfEntries());
        buffer.writeInt(obj.getComponentOverlay());
        
        return buffer;
    }

    @Override
    public SearchComponentUpdate decode(SerializationContext serializationContext, ByteBuf buffer) throws SerializerException, SerializationContext.MissingException {

        int nodeId = buffer.readInt();
        int partitionId = buffer.readInt();
        int partitionDepth = buffer.readInt();
        long entries = buffer.readLong();
        int componentOverlayId = buffer.readInt();
        
        return new SearchComponentUpdate(nodeId, partitionId, partitionDepth, entries, componentOverlayId);
        
    }

    @Override
    public int getSize(SerializationContext serializationContext, SearchComponentUpdate searchComponentUpdate) throws SerializerException, SerializationContext.MissingException {
        int size = 0;
        size += 4* (Integer.SIZE/8);
        size += (Long.SIZE/8);
        
        return size;
    }
}
