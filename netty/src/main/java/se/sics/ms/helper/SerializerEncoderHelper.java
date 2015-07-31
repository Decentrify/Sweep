package se.sics.ms.helper;

import com.google.common.io.BaseEncoding;
import io.netty.buffer.ByteBuf;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.ms.types.IndexEntry;
import se.sics.p2ptoolbox.util.helper.EncodingException;

import static se.sics.p2ptoolbox.util.helper.UserEncoderFactory.*;
import java.io.UnsupportedEncodingException;
import java.util.Collection;

/**
 * Helper class for the encoding of the
 * Created by babbar on 2015-04-21.
 */
public class SerializerEncoderHelper {


    public static void writeIndexEntry(ByteBuf buffer, IndexEntry indexEntry)  throws EncodingException {

        writeStringLength256(buffer, indexEntry.getGlobalId());
        buffer.writeLong(indexEntry.getId());
        writeStringLength256(buffer, indexEntry.getUrl());
        writeStringLength256(buffer, indexEntry.getFileName());
        buffer.writeLong(indexEntry.getFileSize());
        buffer.writeLong(indexEntry.getUploaded().getTime());
        writeStringLength256(buffer, indexEntry.getLanguage());
        buffer.writeInt(indexEntry.getCategory().ordinal());
        writeStringLength65536(buffer, indexEntry.getDescription());
        writeStringLength65536(buffer, indexEntry.getHash());
        if(indexEntry.getLeaderId() == null)
            writeStringLength65536(buffer, new String());
        else
            writeStringLength65536(buffer, BaseEncoding.base64().encode(indexEntry.getLeaderId().getEncoded()));
    }



    /**
     * Simple helper method to write a collection
     * to byte buffer.
     *
     * @param objCollection
     * @param serializer
     * @param buf
     */
    public static void collectionToBuff(Collection objCollection, Serializer serializer, ByteBuf buf){

        int size = objCollection.size();
        buf.writeInt(size);

        for(Object obj : objCollection){
            serializer.toBinary(obj, buf);
        }
    }


    /**
     * In cases where the object could be null, extra information
     * needs to be placed before writing the object.
     *
     * @param buf buffer
     * @param obj object
     */
    public static void checkNullAndUpdateBuff(ByteBuf buf, Object obj){
        
        if(obj == null){
            buf.writeBoolean(true);
        }
        else{
            buf.writeBoolean(false);
        }
        
    }
    

}
