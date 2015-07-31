package se.sics.ms.serializer;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.ms.configuration.MsConfig;
import se.sics.ms.types.IndexEntry;
import se.sics.p2ptoolbox.util.helper.DecodingException;
import se.sics.p2ptoolbox.util.helper.EncodingException;
import se.sics.p2ptoolbox.util.helper.UserDecoderFactory;
import se.sics.p2ptoolbox.util.helper.UserEncoderFactory;

import java.security.PublicKey;
import java.util.Date;

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
    public void toBinary(Object o, ByteBuf buffer) {

        try{
            IndexEntry indexEntry = (IndexEntry)o;

            UserEncoderFactory.writeStringLength256(buffer, indexEntry.getGlobalId());
            buffer.writeLong(indexEntry.getId());
            UserEncoderFactory.writeStringLength256(buffer, indexEntry.getUrl());
            UserEncoderFactory.writeStringLength256(buffer, indexEntry.getFileName());
            buffer.writeLong(indexEntry.getFileSize());
            buffer.writeLong(indexEntry.getUploaded().getTime());
            UserEncoderFactory.writeStringLength256(buffer, indexEntry.getLanguage());
            buffer.writeInt(indexEntry.getCategory().ordinal());
            UserEncoderFactory.writeStringLength65536(buffer, indexEntry.getDescription());
            UserEncoderFactory.writeStringLength65536(buffer, indexEntry.getHash());
            Serializers.lookupSerializer(PublicKey.class).toBinary(indexEntry.getLeaderId(), buffer);
        }
        catch (EncodingException e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }

    }

    @Override
    public Object fromBinary(ByteBuf buffer, Optional<Object> optional) {

        try {

            String gId = UserDecoderFactory.readStringLength256(buffer);
            Long id = buffer.readLong();
            String url = UserDecoderFactory.readStringLength256(buffer);
            String fileName = UserDecoderFactory.readStringLength256(buffer);
            Long fileSize = buffer.readLong();
            Date uploaded =  new Date(buffer.readLong());
            String language = UserDecoderFactory.readStringLength256(buffer);
            MsConfig.Categories category = MsConfig.Categories.values()[buffer.readInt()];
            String description = UserDecoderFactory.readStringLength65536(buffer);
            String hash = UserDecoderFactory.readStringLength65536(buffer);
            PublicKey pub = (PublicKey) Serializers.lookupSerializer(PublicKey.class).fromBinary(buffer, optional);

            return new IndexEntry(gId, id, url, fileName, fileSize, uploaded, language, category, description, hash, pub);

        } catch (DecodingException e) {

            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }
    }
}
