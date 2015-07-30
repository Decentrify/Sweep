package se.sics.ms.serializer;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import se.sics.gvod.net.VodAddress;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.ms.helper.SerializerDecoderHelper;
import se.sics.ms.helper.SerializerEncoderHelper;
import se.sics.ms.util.PartitionHelper;
import se.sics.p2ptoolbox.util.helper.DecodingException;
import se.sics.p2ptoolbox.util.helper.EncodingException;

import java.security.PublicKey;
import java.util.UUID;

/**
 * Serializer for the partition info.
 * Created by babbar on 2015-04-21.
 */
public class PartitionInfoSerializer implements Serializer{


    private final int id;

    public PartitionInfoSerializer(int id) {
        this.id = id;

    }

    @Override
    public int identifier() {
        return this.id;
    }

    @Override
    public void toBinary(Object o, ByteBuf byteBuf) {

        PartitionHelper.PartitionInfo partitionInfo = (PartitionHelper.PartitionInfo)o;

        try {
            Serializers.lookupSerializer(UUID.class).toBinary(partitionInfo.getRequestId(), byteBuf);
            byteBuf.writeLong(partitionInfo.getMedianId());
            byteBuf.writeInt(partitionInfo.getPartitioningTypeInfo().ordinal());
            SerializerEncoderHelper.writeStringLength65536(byteBuf, partitionInfo.getHash());
            Serializers.lookupSerializer(PublicKey.class).toBinary(partitionInfo.getKey(), byteBuf);

        } catch (EncodingException e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public Object fromBinary(ByteBuf byteBuf, Optional<Object> optional) {

        try {
            UUID requestId = (UUID) Serializers.lookupSerializer(UUID.class).fromBinary(byteBuf, optional);
            long medianId = byteBuf.readLong();
            VodAddress.PartitioningType type = VodAddress.PartitioningType.values()[byteBuf.readInt()];
            String hash = SerializerDecoderHelper.readStringLength65536(byteBuf);
            PublicKey key = (PublicKey) Serializers.lookupSerializer(PublicKey.class).fromBinary(byteBuf, optional);

            return new PartitionHelper.PartitionInfo(medianId, requestId, type, hash, key);

        } catch (DecodingException e) {
            e.printStackTrace();
        }


        return null;
    }
}
