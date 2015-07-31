package se.sics.ms.helper;

import com.google.common.base.Optional;
import com.google.common.io.BaseEncoding;
import io.netty.buffer.ByteBuf;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.ms.configuration.MsConfig;
import se.sics.ms.types.IndexEntry;
import se.sics.p2ptoolbox.util.helper.DecodingException;

import  static se.sics.p2ptoolbox.util.helper.UserDecoderFactory.*;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.util.Collection;
import java.util.Date;

/**
 *
 * Decoder Helper for the serializer.
 *
 * Created by babbar on 2015-04-21.
 */
public class SerializerDecoderHelper {



    public static IndexEntry readIndexEntry(ByteBuf buffer)
            throws DecodingException {

        String gId = readStringLength256(buffer);
        Long id = buffer.readLong();
        String url = readStringLength256(buffer);
        String fileName = readStringLength256(buffer);
        Long fileSize = buffer.readLong();
        Date uploaded =  new Date(buffer.readLong());
        String language = readStringLength256(buffer);
        MsConfig.Categories category = MsConfig.Categories.values()[buffer.readInt()];
        String description = readStringLength65536(buffer);
        String hash = readStringLength65536(buffer);
        String leaderId = readStringLength65536(buffer);
        if (leaderId == null)
            return new IndexEntry(gId, id, url, fileName, fileSize, uploaded, language, category, description, hash, null);

        KeyFactory keyFactory;
        PublicKey pub = null;
        try {
            keyFactory = KeyFactory.getInstance("RSA");
            byte[] decode = BaseEncoding.base64().decode(leaderId);
            X509EncodedKeySpec publicKeySpec = new X509EncodedKeySpec(decode);
            pub = keyFactory.generatePublic(publicKeySpec);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (InvalidKeySpecException e) {
            e.printStackTrace();
        }

        return new IndexEntry(gId, id, url, fileName, fileSize, uploaded, language, category, description, hash, pub);
    }



    /**
     * Read the collection from the buffer.
     *
     * @param objCollection objCollection
     * @param buffer buffer
     */
    public static void readCollectionFromBuff(Collection objCollection, Serializer serializer, ByteBuf buffer, Optional<Object> hint){

        int size = buffer.readInt();

        while(size > 0) {
            Object entry = serializer.fromBinary(buffer, hint);
            objCollection.add(entry);
            size--;
        }
    }


    /**
     * Check if the value of the object is committed as null.
     * If null, then move to next object in queue to be read.
     *
     * @param buf buffer
     * @return boolean
     */
    public static boolean checkNullCommit(ByteBuf buf){
        return buf.readBoolean();
    }

}
