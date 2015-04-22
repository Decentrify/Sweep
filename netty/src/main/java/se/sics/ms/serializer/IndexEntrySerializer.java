package se.sics.ms.serializer;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import org.apache.commons.codec.binary.Base64;
import se.sics.gvod.common.msgs.MessageDecodingException;
import se.sics.gvod.common.msgs.MessageEncodingException;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.ms.configuration.MsConfig;
import se.sics.ms.helper.SerializerDecoderHelper;
import se.sics.ms.helper.SerializerEncoderHelper;
import se.sics.ms.types.IndexEntry;
import sun.misc.BASE64Encoder;

import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
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

            SerializerEncoderHelper.writeStringLength256(buffer, indexEntry.getGlobalId());
            buffer.writeLong(indexEntry.getId());
            SerializerEncoderHelper.writeStringLength256(buffer, indexEntry.getUrl());
            SerializerEncoderHelper.writeStringLength256(buffer, indexEntry.getFileName());
            buffer.writeLong(indexEntry.getFileSize());
            buffer.writeLong(indexEntry.getUploaded().getTime());
            SerializerEncoderHelper.writeStringLength256(buffer, indexEntry.getLanguage());
            buffer.writeInt(indexEntry.getCategory().ordinal());
            SerializerEncoderHelper.writeStringLength65536(buffer, indexEntry.getDescription());
            SerializerEncoderHelper.writeStringLength65536(buffer, indexEntry.getHash());
            Serializers.lookupSerializer(PublicKey.class).toBinary(indexEntry.getLeaderId(), buffer);
        }
        catch (MessageEncodingException e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }

    }

    @Override
    public Object fromBinary(ByteBuf buffer, Optional<Object> optional) {

        try {

            String gId = SerializerDecoderHelper.readStringLength256(buffer);
            Long id = buffer.readLong();
            String url = SerializerDecoderHelper.readStringLength256(buffer);
            String fileName = SerializerDecoderHelper.readStringLength256(buffer);
            Long fileSize = buffer.readLong();
            Date uploaded =  new Date(buffer.readLong());
            String language = SerializerDecoderHelper.readStringLength256(buffer);
            MsConfig.Categories category = MsConfig.Categories.values()[buffer.readInt()];
            String description = SerializerDecoderHelper.readStringLength65536(buffer);
            String hash = SerializerDecoderHelper.readStringLength65536(buffer);
            PublicKey pub = (PublicKey) Serializers.lookupSerializer(PublicKey.class).fromBinary(buffer, optional);

            return new IndexEntry(gId, id, url, fileName, fileSize, uploaded, language, category, description, hash, pub);

        } catch (MessageDecodingException e) {

            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }
    }
}
