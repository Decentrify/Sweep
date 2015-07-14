package se.sics.ms.serializer;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.ms.types.ApplicationEntry;
import se.sics.ms.types.IndexEntry;

/**
 * Serializer for the application entry in the system.
 *
 * Created by babbar on 2015-06-21.
 */
public class ApplicationEntrySerializer implements Serializer{

    private int id;

    public ApplicationEntrySerializer(int id){
        this.id = id;
    }

    @Override
    public int identifier() {
        return this.id;
    }

    @Override
    public void toBinary(Object o, ByteBuf buf) {

        ApplicationEntry entry = (ApplicationEntry)o;
        Serializers.lookupSerializer(ApplicationEntry.ApplicationEntryId.class).toBinary(entry.getApplicationEntryId(), buf);
        Serializers.lookupSerializer(IndexEntry.class).toBinary(entry.getEntry(), buf);
    }

    @Override
    public Object fromBinary(ByteBuf buf, Optional<Object> hint) {

        ApplicationEntry.ApplicationEntryId entryId = (ApplicationEntry.ApplicationEntryId) Serializers.lookupSerializer(ApplicationEntry.ApplicationEntryId.class).fromBinary(buf, hint);
        IndexEntry entry = (IndexEntry)Serializers.lookupSerializer(IndexEntry.class).fromBinary(buf, hint);

        return new ApplicationEntry(entryId, entry);
    }
}
