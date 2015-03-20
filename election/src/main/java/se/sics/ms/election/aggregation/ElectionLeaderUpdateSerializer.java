package se.sics.ms.election.aggregation;

import io.netty.buffer.ByteBuf;
import se.sics.p2ptoolbox.serialization.SerializationContext;
import se.sics.p2ptoolbox.serialization.Serializer;

/**
 * Created by babbarshaer on 2015-03-20.
 */
public class ElectionLeaderUpdateSerializer implements Serializer<ElectionLeaderComponentUpdate>{
    @Override
    public ByteBuf encode(SerializationContext serializationContext, ByteBuf byteBuf, ElectionLeaderComponentUpdate electionLeaderComponentUpdate) throws SerializerException, SerializationContext.MissingException {
        
        byteBuf.writeBoolean(electionLeaderComponentUpdate.isLeader());
        byteBuf.writeInt(electionLeaderComponentUpdate.getComponentOverlay());
        return byteBuf;
    }

    @Override
    public ElectionLeaderComponentUpdate decode(SerializationContext serializationContext, ByteBuf byteBuf) throws SerializerException, SerializationContext.MissingException {
        
        boolean isLeader = byteBuf.readBoolean();
        int overlay = byteBuf.readInt();
        
        return new ElectionLeaderComponentUpdate(isLeader, overlay);
    }

    @Override
    public int getSize(SerializationContext serializationContext, ElectionLeaderComponentUpdate electionLeaderComponentUpdate) throws SerializerException, SerializationContext.MissingException {
        int size =0;
        
        size += 1;
        size += (Integer.SIZE/8);
        return size;
    }
}
