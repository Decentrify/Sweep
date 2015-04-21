package se.sics.ms.helper;

import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.msgs.MessageEncodingException;
import se.sics.ms.types.IndexEntry;
import sun.misc.BASE64Encoder;
import java.io.UnsupportedEncodingException;

/**
 * Helper class for the encoding of the
 * Created by babbar on 2015-04-21.
 */
public class SerializerEncoderHelper {




    public static void writeIndexEntry(ByteBuf buffer, IndexEntry indexEntry)  throws MessageEncodingException {
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
            writeStringLength65536(buffer, new BASE64Encoder().encode(indexEntry.getLeaderId().getEncoded()));
    }


    public static void writeStringLength65536(ByteBuf buffer, String str) throws MessageEncodingException {
        if(str == null) {
            writeUnsignedintAsTwoBytes(buffer, 0);
        } else {
            byte[] strBytes;
            try {
                strBytes = str.getBytes("UTF-8");
            } catch (UnsupportedEncodingException var4) {
                throw new MessageEncodingException("Unsupported chartset when encoding string: UTF-8");
            }

            int len = strBytes.length;
            if(len > 1358) {
                throw new MessageEncodingException("Tried to write more bytes to writeString65536 than the MTU size. Attempted to write #bytes: " + len);
            }

            writeUnsignedintAsTwoBytes(buffer, len);
            buffer.writeBytes(strBytes);
        }

    }

    public static void writeStringLength256(ByteBuf buffer, String str) throws MessageEncodingException {
        if(str == null) {
            writeUnsignedintAsOneByte(buffer, 0);
        } else {
            if(str.length() > 255) {
                throw new MessageEncodingException("String length > 255 : " + str);
            }

            byte[] strBytes;
            try {
                strBytes = str.getBytes("UTF-8");
            } catch (UnsupportedEncodingException var4) {
                throw new MessageEncodingException("Unsupported chartset when encoding string: UTF-8");
            }

            int len = strBytes.length;
            writeUnsignedintAsOneByte(buffer, len);
            buffer.writeBytes(strBytes);
        }

    }

    public static void writeUnsignedintAsTwoBytes(ByteBuf buffer, int value) throws MessageEncodingException {
        byte[] result = new byte[2];
        if((double)value < Math.pow(2.0D, 16.0D) && value >= 0) {
            result[0] = (byte)(value >>> 8 & 255);
            result[1] = (byte)(value & 255);
            buffer.writeBytes(result);
        } else {
            throw new MessageEncodingException("writeUnsignedintAsTwoBytes: + Integer value < 0 or " + value + " is larger than 2^31");
        }
    }


    public static void writeUnsignedintAsOneByte(ByteBuf buffer, int value) throws MessageEncodingException {
        if((double)value < Math.pow(2.0D, 8.0D) && value >= 0) {
            buffer.writeByte((byte)(value & 255));
        } else {
            throw new MessageEncodingException("writeUnsignedintAsOneByte: Integer value < 0 or " + value + " is larger than 2^15");
        }
    }



}
