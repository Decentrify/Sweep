package se.sics.p2ptoolbox.election.example.main;

import io.netty.buffer.ByteBuf;
import se.sics.p2ptoolbox.serialization.SerializationContext;
import se.sics.p2ptoolbox.serialization.Serializer;

/**
 * Serializer for the application specific leader descriptor.
 *
 * Created by babbar on 2015-04-02.
 */
public class LEDescriptorSerializer implements Serializer<LeaderDescriptor> {

    @Override
    public ByteBuf encode(SerializationContext serializationContext, ByteBuf byteBuf, LeaderDescriptor leaderDescriptor) throws SerializerException, SerializationContext.MissingException {

        byteBuf.writeInt(leaderDescriptor.utility);
        byteBuf.writeBoolean(leaderDescriptor.membership);

        return byteBuf;
    }

    @Override
    public LeaderDescriptor decode(SerializationContext serializationContext, ByteBuf byteBuf) throws SerializerException, SerializationContext.MissingException {

        int utility = byteBuf.readInt();
        boolean membership = byteBuf.readBoolean();

        return new LeaderDescriptor(utility, membership);
    }

    @Override
    public int getSize(SerializationContext serializationContext, LeaderDescriptor leaderDescriptor) throws SerializerException, SerializationContext.MissingException {

        int size = 0;

        size += Integer.SIZE/8;
        size += Byte.SIZE/8;

        return size;
    }
}
