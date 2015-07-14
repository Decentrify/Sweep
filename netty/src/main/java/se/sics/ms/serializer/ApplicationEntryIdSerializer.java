package se.sics.ms.serializer;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.ms.types.ApplicationEntry;

/**
 * Serializer for the Application Entry Id.
 *
 * Created by babbar on 2015-06-21.
 */
public class ApplicationEntryIdSerializer implements Serializer{

    private int id;

    public ApplicationEntryIdSerializer(int id){
        this.id = id;
    }

    @Override
    public int identifier() {
        return this.id;
    }

    @Override
    public void toBinary(Object o, ByteBuf buf) {

        ApplicationEntry.ApplicationEntryId entryId = (ApplicationEntry.ApplicationEntryId)o;
        buf.writeLong(entryId.getEpochId());
        buf.writeInt(entryId.getLeaderId());
        buf.writeLong(entryId.getEntryId());

    }

    @Override
    public Object fromBinary(ByteBuf buf, Optional<Object> hint) {


        Long epochId = buf.readLong();
        Integer leaderId = buf.readInt();
        Long entryId = buf.readLong();

        return new ApplicationEntry.ApplicationEntryId(epochId, leaderId, entryId);
    }
}
