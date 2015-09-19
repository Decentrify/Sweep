package se.sics.ms.serializer;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.ms.types.OverlayAddress;
import se.sics.ms.types.PeerDescriptor;
import se.sics.p2ptoolbox.util.network.impl.DecoratedAddress;

/**
 * Serializer for the descriptor used by the application.
 *
 * Created by babbar on 2015-04-21.
 */
public class PeerDescriptorSerializer implements Serializer{

    private final int id;

    public PeerDescriptorSerializer(int id){
        this.id = id;
    }

    @Override
    public int identifier() {
        return this.id;
    }

    @Override
    public void toBinary(Object o, ByteBuf byteBuf) {

        PeerDescriptor sd = (PeerDescriptor)o;
        Serializers.lookupSerializer(DecoratedAddress.class).toBinary(sd.getVodAddress(), byteBuf);
        byteBuf.writeInt(sd.getOverlayId().getId());
        byteBuf.writeLong(sd.getNumberOfIndexEntries());
        byteBuf.writeBoolean(sd.isLeaderGroupMember());

    }

    @Override
    public Object fromBinary(ByteBuf byteBuf, Optional<Object> optional) {

        DecoratedAddress decoratedAddress = (DecoratedAddress)Serializers.lookupSerializer(DecoratedAddress.class).fromBinary(byteBuf, optional);
        int overlayId = byteBuf.readInt();
        long numberOfIndexEntries = byteBuf.readLong();
        boolean isLGMember = byteBuf.readBoolean();

        return new PeerDescriptor(new OverlayAddress(decoratedAddress, overlayId), false, numberOfIndexEntries, isLGMember, null);
    }
}
