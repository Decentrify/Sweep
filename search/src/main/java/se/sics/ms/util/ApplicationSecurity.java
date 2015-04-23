package se.sics.ms.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.ms.types.IndexEntry;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.security.*;
import java.util.Formatter;

/**
 * Containing methods used for providing security features for the application.
 *
 * Created by babbarshaer on 2015-04-23.
 */
public class ApplicationSecurity {

    private static Logger logger = LoggerFactory.getLogger(ApplicationSecurity.class);
    
    

    /**
     * Generates a SHA-1 hash on IndexEntry and signs it with a private key
     *
     * @param newEntry
     * @return signed SHA-1 key
     */
    public static String generateSignedHash(IndexEntry newEntry, PrivateKey privateKey) {
        if (newEntry.getLeaderId() == null)
            return null;

        //url
        ByteBuffer dataBuffer = getByteDataFromIndexEntry(newEntry);


        try {
            return generateRSASignature(dataBuffer.array(), privateKey);
        } catch (NoSuchAlgorithmException e) {
            logger.error(e.getMessage());
        } catch (SignatureException e) {
            logger.error(e.getMessage());
        } catch (InvalidKeyException e) {
            logger.error(e.getMessage());
        }

        return null;
    }


    /**
     * Generates the SHA-1 Hash Of the partition update and sign with private key.
     *
     * @param partitionInfo
     * @param privateKey
     * @return signed hash
     */
    public static String generatePartitionInfoSignedHash(PartitionHelper.PartitionInfo partitionInfo, PrivateKey privateKey) {

        if (partitionInfo.getKey() == null)
            return null;

        // generate the byte array from the partitioning data.
        ByteBuffer byteBuffer = getByteDataFromPartitionInfo(partitionInfo);

        // sign the array and return the signed hash value.
        try {
            return generateRSASignature(byteBuffer.array(), privateKey);
        } catch (NoSuchAlgorithmException e) {
            logger.error(e.getMessage());
        } catch (SignatureException e) {
            logger.error(e.getMessage());
        } catch (InvalidKeyException e) {
            logger.error(e.getMessage());
        }

        return null;
    }


    /**
     * Generate the SHA-1 String.
     * TODO: For more efficiency don't convert it to string as it becomes greater than 256bytes and encoding mechanism fails for index hash exchange.
     * FIXME: Change the encoding hash mechanism.
     *
     * @param data
     * @param privateKey
     * @return
     * @throws NoSuchAlgorithmException
     * @throws InvalidKeyException
     * @throws SignatureException
     */
    public static String generateRSASignature(byte[] data, PrivateKey privateKey) throws NoSuchAlgorithmException, InvalidKeyException, SignatureException {
        MessageDigest digest = MessageDigest.getInstance("SHA-1");
        String sha1 = byteArray2Hex(digest.digest(data));

        Signature instance = Signature.getInstance("SHA1withRSA");
        instance.initSign(privateKey);
        instance.update(sha1.getBytes());
        byte[] signature = instance.sign();
        return byteArray2Hex(signature);
    }

    public static String byteArray2Hex(final byte[] hash) {
        Formatter formatter = new Formatter();
        for (byte b : hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    public static boolean isIndexEntrySignatureValid(IndexEntry newEntry) {
        if (newEntry.getLeaderId() == null)
            return false;
        ByteBuffer dataBuffer = getByteDataFromIndexEntry(newEntry);


        try {
            return verifyRSASignature(dataBuffer.array(), newEntry.getLeaderId(), newEntry.getHash());
        } catch (NoSuchAlgorithmException e) {
            logger.error(e.getMessage());
        } catch (SignatureException e) {
            logger.error(e.getMessage());
        } catch (InvalidKeyException e) {
            logger.error(e.getMessage());
        }

        return false;
    }


    /**
     * Verify if the partition update is received from the leader itself only.
     *
     * @param partitionUpdate
     * @return
     */
    public static boolean isPartitionUpdateValid(PartitionHelper.PartitionInfo partitionUpdate) {

        if (partitionUpdate.getKey() == null)
            return false;

        ByteBuffer dataBuffer = getByteDataFromPartitionInfo(partitionUpdate);

        try {
            return verifyRSASignature(dataBuffer.array(), partitionUpdate.getKey(), partitionUpdate.getHash());
        } catch (NoSuchAlgorithmException e) {
            logger.error(e.getMessage());
        } catch (SignatureException e) {
            logger.error(e.getMessage());
        } catch (InvalidKeyException e) {
            logger.error(e.getMessage());
        }

        return false;

    }


    public static ByteBuffer getByteDataFromIndexEntry(IndexEntry newEntry) {
        //url
        byte[] urlBytes;
        if (newEntry.getUrl() != null)
            urlBytes = newEntry.getUrl().getBytes(Charset.forName("UTF-8"));
        else
            urlBytes = new byte[0];

        //filename
        byte[] fileNameBytes;
        if (newEntry.getFileName() != null)
            fileNameBytes = newEntry.getFileName().getBytes(Charset.forName("UTF-8"));
        else
            fileNameBytes = new byte[0];

        //language
        byte[] languageBytes;
        if (newEntry.getLanguage() != null)
            languageBytes = newEntry.getLanguage().getBytes(Charset.forName("UTF-8"));
        else
            languageBytes = new byte[0];

        //description
        byte[] descriptionBytes;
        if (newEntry.getDescription() != null)
            descriptionBytes = newEntry.getDescription().getBytes(Charset.forName("UTF-8"));
        else
            descriptionBytes = new byte[0];

        ByteBuffer dataBuffer;
        if (newEntry.getUploaded() != null)
            dataBuffer = ByteBuffer.allocate(8 * 3 + 4 + urlBytes.length + fileNameBytes.length +
                    languageBytes.length + descriptionBytes.length);
        else
            dataBuffer = ByteBuffer.allocate(8 * 2 + 4 + urlBytes.length + fileNameBytes.length +
                    languageBytes.length + descriptionBytes.length);
        dataBuffer.putLong(newEntry.getId());
        dataBuffer.putLong(newEntry.getFileSize());
        if (newEntry.getUploaded() != null)
            dataBuffer.putLong(newEntry.getUploaded().getTime());
        dataBuffer.putInt(newEntry.getCategory().ordinal());
        if (newEntry.getUrl() != null)
            dataBuffer.put(urlBytes);
        if (newEntry.getFileName() != null)
            dataBuffer.put(fileNameBytes);
        if (newEntry.getLanguage() != null)
            dataBuffer.put(languageBytes);
        if (newEntry.getDescription() != null)
            dataBuffer.put(descriptionBytes);
        return dataBuffer;
    }

    /**
     * Converts the partitioning update in byte array.
     * TODO: Uniform implementation.
     *
     * @param partitionInfo
     * @return partitionInfo byte array.
     */
    public static ByteBuffer getByteDataFromPartitionInfo(PartitionHelper.PartitionInfo partitionInfo) {

        // Decide on a specific order.
        ByteBuffer buffer = ByteBuffer.allocate((3 * 8) + (4));

        // Start filling the buffer with information.
        buffer.putLong(partitionInfo.getMedianId());
        buffer.putLong(partitionInfo.getRequestId().getMostSignificantBits());
        buffer.putLong(partitionInfo.getRequestId().getLeastSignificantBits());

        buffer.putInt(partitionInfo.getPartitioningTypeInfo().ordinal());

        return buffer;
    }

    public static boolean verifyRSASignature(byte[] data, PublicKey key, String signature) throws NoSuchAlgorithmException, InvalidKeyException, SignatureException {
        MessageDigest digest = MessageDigest.getInstance("SHA-1");
        String sha1 = byteArray2Hex(digest.digest(data));

        Signature instance = Signature.getInstance("SHA1withRSA");
        instance.initVerify(key);
        instance.update(sha1.getBytes());
        return instance.verify(hexStringToByteArray(signature));
    }

    public static byte[] hexStringToByteArray(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                    + Character.digit(s.charAt(i + 1), 16));
        }
        return data;
    }
    
    
    
    
    
}
