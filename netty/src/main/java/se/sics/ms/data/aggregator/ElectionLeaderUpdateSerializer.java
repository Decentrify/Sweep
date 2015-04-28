package se.sics.ms.data.aggregator;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import se.sics.kompics.network.netty.serialization.Serializer;

/**
 * Serializer for the leader component update.
 *
 * Created by babbarshaer on 2015-03-20.
 */
public class ElectionLeaderUpdateSerializer implements Serializer {

    private final int id;

    public ElectionLeaderUpdateSerializer(int id){
        this.id = id;
    }

    @Override
    public int identifier() {
        return this.id;
    }

    @Override
    public void toBinary(Object o, ByteBuf byteBuf) {

        ElectionLeaderComponentUpdate componentUpdate = (ElectionLeaderComponentUpdate)o;

        byteBuf.writeBoolean(componentUpdate.isLeader());
        byteBuf.writeInt(componentUpdate.getComponentOverlay());
    }

    @Override
    public Object fromBinary(ByteBuf byteBuf, Optional<Object> optional) {

        boolean isLeader = byteBuf.readBoolean();
        int overlay = byteBuf.readInt();

        return new ElectionLeaderComponentUpdate(isLeader, overlay);
    }
}
