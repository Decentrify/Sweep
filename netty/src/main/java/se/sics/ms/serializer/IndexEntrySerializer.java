package se.sics.ms.serializer;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.msgs.MessageDecodingException;
import se.sics.gvod.common.msgs.MessageEncodingException;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.ms.helper.SerializerDecoderHelper;
import se.sics.ms.helper.SerializerEncoderHelper;
import se.sics.ms.types.IndexEntry;

/**
 * Serializer for the main index entry.
 *
 * Created by babbar on 2015-04-21.
 */
public class IndexEntrySerializer implements Serializer{

    private final int id;

    public IndexEntrySerializer(int id) {

        this.id = id;
    }

    @Override
    public int identifier() {
        return this.id;
    }

    @Override
    public void toBinary(Object o, ByteBuf byteBuf) {

        try{
            IndexEntry entry = (IndexEntry)o;
            SerializerEncoderHelper.writeIndexEntry(byteBuf, entry);
        }
        catch (MessageEncodingException e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }

    }

    @Override
    public Object fromBinary(ByteBuf byteBuf, Optional<Object> optional) {

        try {

            IndexEntry entry = SerializerDecoderHelper.readIndexEntry(byteBuf);
            return entry;

        } catch (MessageDecodingException e) {

            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }
    }
}
