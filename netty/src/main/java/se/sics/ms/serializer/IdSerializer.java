package se.sics.ms.serializer;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.ms.types.Id;

import java.rmi.activation.ActivationGroupID;
import java.security.PublicKey;

/**
 * Serializer for the Id.
 * Created by babbar on 2015-04-21.
 */
public class IdSerializer implements Serializer{

    private final int id;

    public IdSerializer(int id) {
        this.id = id;
    }

    @Override
    public int identifier() {
        return this.id;
    }

    @Override
    public void toBinary(Object o, ByteBuf byteBuf) {
        Id id = (Id)o;
        byteBuf.writeLong(id.getId());
        Serializers.lookupSerializer(PublicKey.class).toBinary(id.getLeaderId(), byteBuf);
    }

    @Override
    public Object fromBinary(ByteBuf byteBuf, Optional<Object> optional) {

        long id = byteBuf.readLong();
        PublicKey leaderPublicKey = (PublicKey) Serializers.lookupSerializer(PublicKey.class).fromBinary(byteBuf, optional);

        return new Id(id, leaderPublicKey);
    }
}
