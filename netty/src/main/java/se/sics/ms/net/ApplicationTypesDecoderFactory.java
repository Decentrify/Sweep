package se.sics.ms.net;

import io.netty.buffer.ByteBuf;
import org.apache.commons.codec.binary.Base64;
import se.sics.gvod.common.VodDescriptor;
import se.sics.gvod.common.msgs.MessageDecodingException;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.util.UserTypesDecoderFactory;
import se.sics.ms.configuration.MsConfig;
import se.sics.ms.types.Id;
import se.sics.ms.types.IndexEntry;
import se.sics.ms.types.IndexHash;
import se.sics.ms.types.SearchPattern;

import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.util.*;

import static se.sics.gvod.net.util.UserTypesDecoderFactory.readVodNodeDescriptor;

/**
 *
 * @author jdowling
 */
public class ApplicationTypesDecoderFactory {

    public static IndexEntry readIndexEntry(ByteBuf buffer)
            throws MessageDecodingException {
        String gId = UserTypesDecoderFactory.readStringLength256(buffer);
        Long id = buffer.readLong();
        String url = UserTypesDecoderFactory.readStringLength256(buffer);
        String fileName = UserTypesDecoderFactory.readStringLength256(buffer);
        Long fileSize = buffer.readLong();
        Date uploaded =  new Date(buffer.readLong());
        String language = UserTypesDecoderFactory.readStringLength256(buffer);
        MsConfig.Categories category = MsConfig.Categories.values()[buffer.readInt()];
        String description = UserTypesDecoderFactory.readStringLength65536(buffer);
        String hash = UserTypesDecoderFactory.readStringLength256(buffer);
        String leaderId = UserTypesDecoderFactory.readStringLength65536(buffer);
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

    public static Collection<IndexEntry> readIndexEntryCollection(ByteBuf buffer) throws MessageDecodingException {
        int len = UserTypesDecoderFactory.readUnsignedIntAsTwoBytes(buffer);
        Collection<IndexEntry> items = new ArrayList<IndexEntry>();
        for (int i = 0; i < len; i++) {
            items.add(readIndexEntry(buffer));
        }
        return items;
    }

    public static IndexHash readIndexEntryHash(ByteBuf buffer) throws MessageDecodingException {
        Id id = readId(buffer);
        String hash = UserTypesDecoderFactory.readStringLength256(buffer);

        return new IndexHash(id, hash);
    }

    public static Collection<IndexHash> readIndexEntryHashCollection(ByteBuf buffer) throws MessageDecodingException {
        int len = UserTypesDecoderFactory.readUnsignedIntAsTwoBytes(buffer);
        List<IndexHash> hashes = new ArrayList<IndexHash>();
        for (int i = 0; i < len; i++) {
            hashes.add(readIndexEntryHash(buffer));
        }
        return hashes;
    }

    public static Id readId(ByteBuf buffer) throws MessageDecodingException {
        Long id = buffer.readLong();
        String leaderId = UserTypesDecoderFactory.readStringLength65536(buffer);
        if (leaderId == null)
            return new Id(id, null);

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

        return new Id(id, pub);
    }

    public static Collection<Id> readIdCollection(ByteBuf buffer) throws MessageDecodingException {
        int len = UserTypesDecoderFactory.readUnsignedIntAsTwoBytes(buffer);
        List<Id> hashes = new ArrayList<Id>();
        for (int i = 0; i < len; i++) {
            hashes.add(readId(buffer));
        }
        return hashes;
    }

    public static Long[] readLongArray(ByteBuf buffer) {
        int len = UserTypesDecoderFactory.readUnsignedIntAsTwoBytes(buffer);
        Long[] items = new Long[len];
        for (int i = 0; i < len; i++) {
            items[i] = buffer.readLong();
        }
        return items;
    }

    public static VodAddress[] readVodAddressArray(ByteBuf buffer) throws MessageDecodingException {
        int len = UserTypesDecoderFactory.readUnsignedIntAsTwoBytes(buffer);
        VodAddress[] items = new VodAddress[len];
        for (int i = 0; i < len; i++) {
            items[i] = UserTypesDecoderFactory.readVodAddress(buffer);
        }
        return items;
    }

    public static SearchPattern readSearchPattern(ByteBuf buffer) throws MessageDecodingException {
        String fileNamePattern = UserTypesDecoderFactory.readStringLength256(buffer);
        int minFileSize = buffer.readInt();
        int maxFileSize = buffer.readInt();
        Date minUploadDate = new Date(buffer.readLong());
        Date maxUploadDate = new Date(buffer.readLong());
        String language = UserTypesDecoderFactory.readStringLength256(buffer);
        MsConfig.Categories category = MsConfig.Categories.values()[buffer.readInt()];
        String descriptionPattern = UserTypesDecoderFactory.readStringLength65536(buffer);

        return new SearchPattern(fileNamePattern, minFileSize, maxFileSize, minUploadDate, maxUploadDate, language, category, descriptionPattern);
    }

    public static Set<VodDescriptor> readVodDescriptorSet(ByteBuf buffer) throws MessageDecodingException {
        int len = UserTypesDecoderFactory.readUnsignedIntAsTwoBytes(buffer);
        Set<VodDescriptor> addrs = new HashSet<VodDescriptor>();
        for (int i = 0; i < len; i++) {
            addrs.add(readVodNodeDescriptor(buffer));
        }
        return addrs;
    }
}
