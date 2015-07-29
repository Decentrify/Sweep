package se.sics.ms.serializer;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.ms.types.ApplicationEntry;
import se.sics.ms.util.IdScorePair;

/**
 * Serializer for the IdScorePair object information.
 *
 * Created by babbarshaer on 2015-07-20.
 */
public class IdScorePairSerializer implements Serializer{

    private int id;

    public IdScorePairSerializer(int id){
        this.id = id;
    }
    
    
    @Override
    public int identifier() {
        return this.id;
    }

    @Override
    public void toBinary(Object o, ByteBuf buf) {

        IdScorePair idScorePair = (IdScorePair)o;
        Serializers.lookupSerializer(ApplicationEntry.ApplicationEntryId.class).toBinary(idScorePair.getEntryId(), buf);
        buf.writeFloat(idScorePair.getScore());
    }

    @Override
    public Object fromBinary(ByteBuf buf, Optional<Object> hint) {

        ApplicationEntry.ApplicationEntryId entryId = (ApplicationEntry.ApplicationEntryId)Serializers.lookupSerializer(ApplicationEntry.ApplicationEntryId.class).fromBinary(buf, hint);
        float score = buf.readFloat();
        
        return new IdScorePair(entryId, score);
    }
}
