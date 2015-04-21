package se.sics.ms.serializer;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.msgs.MessageDecodingException;
import se.sics.gvod.common.msgs.MessageEncodingException;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.ms.configuration.MsConfig;
import se.sics.ms.helper.SerializerDecoderHelper;
import se.sics.ms.helper.SerializerEncoderHelper;
import se.sics.ms.types.IndexEntry;
import se.sics.ms.types.SearchPattern;

import java.util.Date;

/**
 * Serializer for the Search Pattern.
 *
 * Created by babbar on 2015-04-21.
 */
public class SearchPatternSerializer implements Serializer{

    private final int id;

    public SearchPatternSerializer(int id) {
        this.id = id;
    }

    @Override
    public int identifier() {
        return this.id;
    }

    @Override
    public void toBinary(Object o, ByteBuf byteBuf) {

        try {
            SearchPattern searchPattern = (SearchPattern)o;
            SerializerEncoderHelper.writeStringLength256(byteBuf, searchPattern.getFileNamePattern());
            byteBuf.writeInt(searchPattern.getMinFileSize());
            byteBuf.writeInt(searchPattern.getMaxFileSize());
            byteBuf.writeLong(searchPattern.getMinUploadDate().getTime());
            byteBuf.writeLong(searchPattern.getMaxUploadDate().getTime());
            SerializerEncoderHelper.writeStringLength256(byteBuf, searchPattern.getLanguage());
            byteBuf.writeInt(searchPattern.getCategory().ordinal());
            SerializerEncoderHelper.writeStringLength65536(byteBuf, searchPattern.getDescriptionPattern());


        } catch (MessageEncodingException e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }

    }

    @Override
    public Object fromBinary(ByteBuf byteBuf, Optional<Object> optional) {

        try{

            String filePattern = SerializerDecoderHelper.readStringLength256(byteBuf);
            int minFileSize = byteBuf.readInt();
            int maxFileSize = byteBuf.readInt();
            Date minUploadDate = new Date(byteBuf.readLong());
            Date maxUploadDate = new Date(byteBuf.readLong());

            String language = SerializerDecoderHelper.readStringLength256(byteBuf);
            MsConfig.Categories category = MsConfig.Categories.values()[byteBuf.readInt()];
            String description = SerializerDecoderHelper.readStringLength65536(byteBuf);


            return new SearchPattern(filePattern, minFileSize, maxFileSize, minUploadDate, maxUploadDate, language, category, description);

        } catch (MessageDecodingException e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }
    }
}
