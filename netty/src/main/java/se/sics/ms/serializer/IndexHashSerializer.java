package se.sics.ms.serializer;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.msgs.MessageDecodingException;
import se.sics.gvod.common.msgs.MessageEncodingException;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.ms.helper.SerializerDecoderHelper;
import se.sics.ms.helper.SerializerEncoderHelper;
import se.sics.ms.types.Id;
import se.sics.ms.types.IndexHash;

/**
 * Serializer for the Index Hash Object.
 * Created by babbar on 2015-04-24.
 */
public class IndexHashSerializer implements Serializer{

    private final int id;

    public IndexHashSerializer(int id){
        this.id = id;
    }


    @Override
    public int identifier() {
        return this.id;
    }

    @Override
    public void toBinary(Object o, ByteBuf byteBuf) {

        try{
            IndexHash indexHash = (IndexHash)o;
            Serializers.lookupSerializer(Id.class).toBinary(indexHash.getId(), byteBuf);
            SerializerEncoderHelper.writeStringLength65536(byteBuf, indexHash.getHash());
        }
        catch (MessageEncodingException e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage(), e);
        }


    }

    @Override
    public Object fromBinary(ByteBuf byteBuf, Optional<Object> optional) {

        try {

            Id id = (Id) Serializers.lookupSerializer(Id.class).fromBinary(byteBuf, optional);
            String hash = SerializerDecoderHelper.readStringLength65536(byteBuf);
            return new IndexHash(id, hash);

        }
        catch (MessageDecodingException e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
