package se.sics.ms.helper;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import org.apache.commons.codec.binary.Base64;
import se.sics.gvod.common.msgs.MessageDecodingException;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.ms.configuration.MsConfig;
import se.sics.ms.types.IndexEntry;

import java.io.UnsupportedEncodingException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.util.Collection;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * Decoder Helper for the serializer.
 *
 * Created by babbar on 2015-04-21.
 */
public class SerializerDecoderHelper {



    public static IndexEntry readIndexEntry(ByteBuf buffer)
            throws MessageDecodingException {
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
            byte[] decode = Base64.decodeBase64(leaderId.getBytes());
            X509EncodedKeySpec publicKeySpec = new X509EncodedKeySpec(decode);
            pub = keyFactory.generatePublic(publicKeySpec);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (InvalidKeySpecException e) {
            e.printStackTrace();
        }

        return new IndexEntry(gId, id, url, fileName, fileSize, uploaded, language, category, description, hash, pub);
    }



    public static String readStringLength256(ByteBuf buffer) throws MessageDecodingException {
        int len = readIntAsOneByte(buffer);
        return len == 0?null:readString(buffer, len);
    }


    public static int readIntAsOneByte(ByteBuf buffer) throws MessageDecodingException {
        return readUnsignedIntAsOneByte(buffer);
    }

    private static String readString(ByteBuf buffer, int len) throws MessageDecodingException {
        byte[] bytes = new byte[len];
        buffer.readBytes(bytes);

        try {
            return new String(bytes, "UTF-8");
        } catch (UnsupportedEncodingException var4) {
            throw new MessageDecodingException(var4.getMessage());
        }
    }


    public static String readStringLength65536(ByteBuf buffer) throws MessageDecodingException {
        int len = readUnsignedIntAsTwoBytes(buffer);
        return len == 0?null:readString(buffer, len);
    }

    public static int readUnsignedIntAsOneByte(ByteBuf buffer) {
        byte value = buffer.readByte();
        return value & 255;
    }

    public static int readUnsignedIntAsTwoBytes(ByteBuf buffer) {
        byte[] bytes = new byte[2];
        buffer.readBytes(bytes);
        int temp0 = bytes[0] & 255;
        int temp1 = bytes[1] & 255;
        return (temp0 << 8) + temp1;
    }

    /**
     * Read the collection from the buffer.
     *
     * @param objCollection
     * @param buffer
     */
    public static void readCollectionFromBuff(Collection objCollection, Serializer serializer, ByteBuf buffer, Optional<Object> hint){

        int size = buffer.readInt();

        while(size > 0) {
            Object entry = serializer.fromBinary(buffer, hint);
            objCollection.add(entry);
        }
    }

}
