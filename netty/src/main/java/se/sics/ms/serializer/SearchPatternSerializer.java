package se.sics.ms.serializer;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.ms.configuration.MsConfig;
import se.sics.ms.types.SearchPattern;
import se.sics.p2ptoolbox.util.helper.DecodingException;
import se.sics.p2ptoolbox.util.helper.EncodingException;
import se.sics.p2ptoolbox.util.helper.UserDecoderFactory;
import se.sics.p2ptoolbox.util.helper.UserEncoderFactory;

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
            UserEncoderFactory.writeStringLength256(byteBuf, searchPattern.getFileNamePattern());
            byteBuf.writeInt(searchPattern.getMinFileSize());
            byteBuf.writeInt(searchPattern.getMaxFileSize());

            if(searchPattern.getMinUploadDate() == null)
                byteBuf.writeLong(0);
            else
                byteBuf.writeLong(searchPattern.getMinUploadDate().getTime());

            if(searchPattern.getMaxUploadDate() == null)
                byteBuf.writeLong(0);
            else
                byteBuf.writeLong(searchPattern.getMaxUploadDate().getTime());

            UserEncoderFactory.writeStringLength256(byteBuf, searchPattern.getLanguage());
            byteBuf.writeInt(searchPattern.getCategory().ordinal());
            UserEncoderFactory.writeStringLength65536(byteBuf, searchPattern.getDescriptionPattern());


        } catch (EncodingException e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }

    }

    @Override
    public Object fromBinary(ByteBuf byteBuf, Optional<Object> optional) {

        try{

            String filePattern = UserDecoderFactory.readStringLength256(byteBuf);
            int minFileSize = byteBuf.readInt();
            int maxFileSize = byteBuf.readInt();
            Date minUploadDate;
            long minUploadDateTime = byteBuf.readLong();

            if(minUploadDateTime == 0)
                minUploadDate = null;
            else
                minUploadDate= new Date(minUploadDateTime);

            Date maxUploadDate;
            long maxUploadDateTime = byteBuf.readLong();

            if(maxUploadDateTime == 0)
                maxUploadDate = null;
            else
                maxUploadDate= new Date(maxUploadDateTime);

            String language = UserDecoderFactory.readStringLength256(byteBuf);
            MsConfig.Categories category = MsConfig.Categories.values()[byteBuf.readInt()];
            String description = UserDecoderFactory.readStringLength65536(byteBuf);


            return new SearchPattern(filePattern, minFileSize, maxFileSize, minUploadDate, maxUploadDate, language, category, description);

        } catch (DecodingException e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }
    }
}
