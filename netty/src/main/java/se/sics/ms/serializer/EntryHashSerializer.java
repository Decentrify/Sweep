package se.sics.ms.serializer;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.ms.helper.SerializerDecoderHelper;
import se.sics.ms.helper.SerializerEncoderHelper;
import se.sics.ms.types.ApplicationEntry;
import se.sics.ms.types.EntryHash;
import se.sics.p2ptoolbox.util.helper.DecodingException;
import se.sics.p2ptoolbox.util.helper.EncodingException;

import java.security.PublicKey;

/**
 * Serializer for the hash container for the
 * application entry. Contain condensed data for the application entry.
 *
 * Created by babbar on 2015-06-21.
 */
public class EntryHashSerializer implements Serializer{

    private int id;

    public EntryHashSerializer(int id){
        this.id = id;
    }


    @Override
    public int identifier() {
        return this.id;
    }

    @Override
    public void toBinary(Object o, ByteBuf buf) {

        try {
            EntryHash entryHash = (EntryHash)o;
            Serializers.lookupSerializer(ApplicationEntry.ApplicationEntryId.class).toBinary(entryHash.getEntryId(), buf);
            Serializers.lookupSerializer(PublicKey.class).toBinary(entryHash.getLeaderKey(), buf);
            SerializerEncoderHelper.writeStringLength65536(buf, entryHash.getHash());

        } catch (EncodingException e) {
            e.printStackTrace();
            throw new RuntimeException("Serialization (Encoding) Failed: " + this.getClass(), e);
        }
    }

    @Override
    public Object fromBinary(ByteBuf buf, Optional<Object> hint) {

        try {

            ApplicationEntry.ApplicationEntryId entryId = (ApplicationEntry.ApplicationEntryId) Serializers.lookupSerializer(ApplicationEntry.ApplicationEntryId.class)
                    .fromBinary(buf, hint);

            PublicKey leaderKey = (PublicKey)Serializers.lookupSerializer(PublicKey.class).fromBinary(buf, hint);
            String hash = SerializerDecoderHelper.readStringLength65536(buf);
            return new EntryHash(entryId, leaderKey, hash);

        } catch (DecodingException e) {
            e.printStackTrace();
            throw new RuntimeException("Serialization (Decoding) Failed: " + this.getClass(), e);
        }
    }

}
