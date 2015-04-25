package se.sics.ms.serializer;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.msgs.MessageDecodingException;
import se.sics.gvod.common.msgs.MessageEncodingException;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.ms.helper.SerializerDecoderHelper;
import se.sics.ms.helper.SerializerEncoderHelper;
import se.sics.ms.util.PartitionHelper;

import java.util.UUID;

/**
 * Serializer for the partition information hash object.
 *  
 * Created by babbarshaer on 2015-04-25.
 */
public class PartitionInfoHashSerializer implements Serializer {
    
    private final int id;
    
    public PartitionInfoHashSerializer(int id){
        this.id = id;
    }
    
    @Override
    public int identifier() {
        return this.id;
    }

    @Override
    public void toBinary(Object o, ByteBuf byteBuf) {
        
        try {
            PartitionHelper.PartitionInfoHash hashObject = (PartitionHelper.PartitionInfoHash)o;
            Serializers.lookupSerializer(UUID.class).toBinary(hashObject.getPartitionRequestId(), byteBuf);
            SerializerEncoderHelper.writeStringLength65536(byteBuf, hashObject.getHash());
            
        } catch (MessageEncodingException e) {
            
            e.printStackTrace();
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public Object fromBinary(ByteBuf byteBuf, Optional<Object> optional) {
        
        try {
            UUID partitionRequestId = (UUID)Serializers.lookupSerializer(UUID.class).fromBinary(byteBuf, optional);
            String hash = SerializerDecoderHelper.readStringLength65536(byteBuf);
            return new PartitionHelper.PartitionInfoHash(partitionRequestId, hash);
            
        } catch (MessageDecodingException e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
