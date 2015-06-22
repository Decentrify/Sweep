package se.sics.ms.serializer;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.ms.types.OverlayId;

/**
 * Serializer for the overlayid in the system.
 *
 * Created by babbar on 2015-06-22.
 */
public class OverlayIdSerializer implements Serializer{

    private int id;

    public OverlayIdSerializer(int id){
        this.id = id;
    }

    @Override
    public int identifier() {
        return this.id;
    }

    @Override
    public void toBinary(Object o, ByteBuf buf) {

        OverlayId overlayId = (OverlayId)o;
        buf.writeInt(overlayId.getId());
    }

    @Override
    public Object fromBinary(ByteBuf buf, Optional<Object> hint) {

        int overlayId = buf.readInt();
        return new OverlayId(overlayId);
    }
}
