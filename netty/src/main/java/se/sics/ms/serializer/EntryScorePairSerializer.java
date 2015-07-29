package se.sics.ms.serializer;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.ms.types.ApplicationEntry;
import se.sics.ms.util.EntryScorePair;
import se.sics.ms.util.IdScorePair;

/**
 * Serializer for the IdScorePair object information.
 *
 * Created by babbarshaer on 2015-07-20.
 */
public class EntryScorePairSerializer implements Serializer{

    private int id;

    public EntryScorePairSerializer(int id){
        this.id = id;
    }
    
    
    @Override
    public int identifier() {
        return this.id;
    }

    @Override
    public void toBinary(Object o, ByteBuf buf) {

        EntryScorePair entryScorePair = (EntryScorePair)o;
        Serializers.lookupSerializer(ApplicationEntry.class).toBinary(entryScorePair.getEntry(), buf);
        buf.writeFloat(entryScorePair.getScore());
    }

    @Override
    public Object fromBinary(ByteBuf buf, Optional<Object> hint) {

        ApplicationEntry entry = (ApplicationEntry)Serializers.lookupSerializer(ApplicationEntry.class).fromBinary(buf, hint);
        float score = buf.readFloat();
        
        return new EntryScorePair(entry, score);
    }
}
