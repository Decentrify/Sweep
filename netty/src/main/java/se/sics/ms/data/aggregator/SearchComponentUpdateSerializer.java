package se.sics.ms.data.aggregator;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import se.sics.kompics.network.netty.serialization.Serializer;

/**
 * Serializer for the search component update.
 * Created by babbarshaer on 2015-03-20.
 */
public class SearchComponentUpdateSerializer implements Serializer {

    private final int id;
    public SearchComponentUpdateSerializer(int id){
        this.id = id;
    }

    @Override
    public int identifier() {
        return this.id;
    }

    @Override
    public void toBinary(Object o, ByteBuf buffer) {

        SearchComponentUpdate obj = (SearchComponentUpdate)o;

        buffer.writeInt(obj.getNodeId());
        buffer.writeInt(obj.getPartitionId());
        buffer.writeInt(obj.getPartitionDepth());
        buffer.writeLong(obj.getNumberOfEntries());
        buffer.writeInt(obj.getComponentOverlay());

    }

    @Override
    public Object fromBinary(ByteBuf buffer, Optional<Object> optional) {

        int nodeId = buffer.readInt();
        int partitionId = buffer.readInt();
        int partitionDepth = buffer.readInt();
        long entries = buffer.readLong();
        int componentOverlayId = buffer.readInt();

        return new SearchComponentUpdate(nodeId, partitionId, partitionDepth, entries, componentOverlayId);
    }
}
