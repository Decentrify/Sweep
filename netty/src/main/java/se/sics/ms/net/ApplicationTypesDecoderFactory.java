package se.sics.ms.net;

import io.netty.buffer.ByteBuf;
import org.apache.commons.codec.binary.Base64;
import se.sics.gvod.common.msgs.MessageDecodingException;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.util.UserTypesDecoderFactory;
import se.sics.gvod.timer.TimeoutId;
import se.sics.ms.configuration.MsConfig;
import se.sics.ms.types.*;
import se.sics.ms.util.PartitionHelper;

import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.util.*;

import static se.sics.gvod.net.util.UserTypesDecoderFactory.*;

//import se.sics.ms.messages.PartitioningMessage;

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
        String hash = UserTypesDecoderFactory.readStringLength65536(buffer);
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
        String hash = UserTypesDecoderFactory.readStringLength65536(buffer);

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

        // Handling the checks for the case in which the date is null, so the long value written is 0.
        Date minUploadDate;
        long minUploadDateTime = buffer.readLong();

        if(minUploadDateTime == 0)
            minUploadDate = null;
        else
            minUploadDate= new Date(minUploadDateTime);

        Date maxUploadDate;
        long maxUploadDateTime = buffer.readLong();

        if(maxUploadDateTime == 0)
            maxUploadDate = null;
        else
            maxUploadDate= new Date(maxUploadDateTime);

        String language = UserTypesDecoderFactory.readStringLength256(buffer);
        MsConfig.Categories category = MsConfig.Categories.values()[buffer.readInt()];
        String descriptionPattern = UserTypesDecoderFactory.readStringLength65536(buffer);

        return new SearchPattern(fileNamePattern, minFileSize, maxFileSize, minUploadDate, maxUploadDate, language, category, descriptionPattern);
    }

    public static Set<PeerDescriptor> readSearchDescriptorSet(ByteBuf buffer) throws MessageDecodingException {
        int len = UserTypesDecoderFactory.readUnsignedIntAsTwoBytes(buffer);
        Set<PeerDescriptor> addrs = new HashSet<PeerDescriptor>();
        for (int i = 0; i < len; i++) {
            PeerDescriptor currentSearchDescriptor = readSearchDescriptor(buffer);
            addrs.add(currentSearchDescriptor);
        }
        return addrs;
    }

    /**
     * Based on the buffer passed, create a search descriptor instance.
     * @param byteBuf buffer
     * @return decoded search descriptor
     *
     * @throws MessageDecodingException
     */
    public static PeerDescriptor readSearchDescriptor(ByteBuf byteBuf) throws MessageDecodingException {
        
        PeerDescriptor descriptor;
        
        VodAddress vodAddress = UserTypesDecoderFactory.readVodAddress(byteBuf);
        long numberOfIndexEntries = byteBuf.readLong();
        boolean isLGMember = byteBuf.readBoolean();
        
        // TODO : FIX this deserializer.
        descriptor = new PeerDescriptor(new OverlayAddress(null, 0), false, numberOfIndexEntries, isLGMember, null);
        return descriptor;
    }


    /**
     * Required for decoding the partition update sequence.
     *
     * @param buffer
     * @return
     * @throws MessageDecodingException
     */
    public static LinkedList<PartitionHelper.PartitionInfo> readPartitionUpdateSequence(ByteBuf buffer) throws MessageDecodingException {

        int len = UserTypesDecoderFactory.readUnsignedIntAsTwoBytes(buffer);
        LinkedList<PartitionHelper.PartitionInfo> partitionUpdates = new LinkedList<PartitionHelper.PartitionInfo>();
        for(int i = 0; i < len; i++){
            partitionUpdates.add(readPartitionUpdate(buffer));
        }
        return partitionUpdates;
    }

    /**
     *
     * @param buffer
     * @return list of partition request ids.
     * @throws MessageDecodingException
     */
    public static List<TimeoutId> readPartitionUpdateRequestId(ByteBuf buffer) throws MessageDecodingException {

        int len = UserTypesDecoderFactory.readUnsignedIntAsTwoBytes(buffer);
        List<TimeoutId> partitionRequestIds = new ArrayList<TimeoutId>();

        for(int i =0 ; i< len ; i++){
            partitionRequestIds.add(UserTypesDecoderFactory.readTimeoutId(buffer));
        }

        return partitionRequestIds;
    }

    /**
     * Required for decoding a single partition update during the two phase partition commit.
     * <i> Sequence should be same as of encoding. </i>
     *
     * @param buffer
     * @return
     * @throws MessageDecodingException
     */
    public static PartitionHelper.PartitionInfo readPartitionUpdate(ByteBuf buffer) throws MessageDecodingException {

        long middleEntryId = buffer.readLong();
        TimeoutId requestId = UserTypesDecoderFactory.readTimeoutId(buffer);
        int partitionsNumber = buffer.readInt();

        // Added decoding support for the hash and public key of the leader.

        String hash = readStringLength65536(buffer);
        String stringKey = readStringLength65536(buffer);
        if (stringKey == null)
            return new PartitionHelper.PartitionInfo(middleEntryId, null, VodAddress.PartitioningType.values()[partitionsNumber], hash, null);

        KeyFactory keyFactory;
        PublicKey pub = null;
        try {
            keyFactory = KeyFactory.getInstance("RSA");
            byte[] decode = Base64.decodeBase64(stringKey.getBytes());
            X509EncodedKeySpec publicKeySpec = new X509EncodedKeySpec(decode);
            pub = keyFactory.generatePublic(publicKeySpec);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (InvalidKeySpecException e) {
            e.printStackTrace();
        }

        return new PartitionHelper.PartitionInfo(middleEntryId, null , VodAddress.PartitioningType.values()[partitionsNumber], hash, pub);
    }


    /**
     * Read the partition info hash sequence.
     * @param buffer
     * @return PartitionInfoHash Sequence.
     * @throws MessageDecodingException
     */
    public static LinkedList<PartitionHelper.PartitionInfoHash> readPartitionUpdateHashSequence(ByteBuf buffer) throws MessageDecodingException {

        int len = UserTypesDecoderFactory.readUnsignedIntAsTwoBytes(buffer);

        LinkedList<PartitionHelper.PartitionInfoHash> partitionUpdates = new LinkedList<PartitionHelper.PartitionInfoHash>();
        for(int i = 0; i < len; i++){
            partitionUpdates.add(readPartitionUpdateHash(buffer));
        }
        return partitionUpdates;
    }


    /**
     * Read the partition info hash object.
     * @param buffer
     * @return
     * @throws MessageDecodingException
     */
    public static PartitionHelper.PartitionInfoHash readPartitionUpdateHash(ByteBuf buffer) throws MessageDecodingException {

        TimeoutId partitionUpdateId = UserTypesDecoderFactory.readTimeoutId(buffer);
        String hash = UserTypesDecoderFactory.readStringLength65536(buffer);
        return new PartitionHelper.PartitionInfoHash(null, hash);
    }

    public static PublicKey readPublicKey(ByteBuf buffer) throws MessageDecodingException {

        String key = UserTypesDecoderFactory.readStringLength65536(buffer);

        if(key == null){
            return null;
        }

        KeyFactory keyFactory;
        PublicKey pub = null;
        try {
            keyFactory = KeyFactory.getInstance("RSA");
            byte[] decode = Base64.decodeBase64(key.getBytes());
            X509EncodedKeySpec publicKeySpec = new X509EncodedKeySpec(decode);
            pub = keyFactory.generatePublic(publicKeySpec);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (InvalidKeySpecException e) {
            e.printStackTrace();
        }

        return pub;
    }

    public static OverlayId readOverlayId(ByteBuf buffer) {

        int overlayId = buffer.readInt();

        return new OverlayId(overlayId);

    }

}
