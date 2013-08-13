/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.ms.net;

import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.VodDescriptor;
import se.sics.gvod.common.msgs.MessageEncodingException;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.util.UserTypesEncoderFactory;
import se.sics.ms.types.IndexEntry;
import se.sics.ms.types.SearchPattern;
import sun.misc.BASE64Encoder;

import java.util.Set;

import static se.sics.gvod.net.util.UserTypesEncoderFactory.*;

/**
 *
 * @author jdowling
 */
public class ApplicationTypesEncoderFactory {
    
    public static void writeIndexEntry(ByteBuf buffer, IndexEntry indexEntry)
            throws MessageEncodingException {
        buffer.writeLong(indexEntry.getId());
        writeStringLength256(buffer, indexEntry.getUrl());
        writeStringLength256(buffer, indexEntry.getFileName());
        buffer.writeLong(indexEntry.getFileSize());
        buffer.writeLong(indexEntry.getUploaded().getTime());
        writeStringLength256(buffer, indexEntry.getLanguage());
        buffer.writeInt(indexEntry.getCategory().ordinal());
        writeStringLength65536(buffer, indexEntry.getDescription());
        writeStringLength256(buffer, indexEntry.getHash());
        if(indexEntry.getLeaderId() == null)
            writeStringLength65536(buffer, new String());
        else
            writeStringLength65536(buffer, new BASE64Encoder().encode(indexEntry.getLeaderId().getEncoded()));
    }

    public static void writeIndexEntryArray(ByteBuf buffer, IndexEntry[] items) throws MessageEncodingException {
        if(items == null){
            writeUnsignedintAsOneByte(buffer, 0);
            return;
        }
        UserTypesEncoderFactory.writeUnsignedintAsTwoBytes(buffer, items.length);
        for(IndexEntry item : items)
            writeIndexEntry(buffer, item);
    }

    public static void writeLongArray(ByteBuf buffer, Long[] items) throws MessageEncodingException {
        if (items == null) {
            writeUnsignedintAsOneByte(buffer, 0);
            return;
        }
        UserTypesEncoderFactory.writeUnsignedintAsTwoBytes(buffer, items.length);
        for (Long item : items)
            buffer.writeLong(item);
    }

    public static void writeVodAddressArray(ByteBuf buffer, VodAddress[] items) throws MessageEncodingException {
        if(items == null) {
            writeUnsignedintAsOneByte(buffer, 0);
            return;
        }

        UserTypesEncoderFactory.writeUnsignedintAsTwoBytes(buffer, items.length);
        for(VodAddress item : items)
            writeVodAddress(buffer, item);
    }

    public static void writeSearchPattern(ByteBuf buffer, SearchPattern pattern) throws MessageEncodingException {
        writeStringLength256(buffer, pattern.getFileNamePattern());
        buffer.writeInt(pattern.getMinFileSize());
        buffer.writeInt(pattern.getMaxFileSize());
        buffer.writeLong(pattern.getMinUploadDate().getTime());
        buffer.writeLong(pattern.getMaxUploadDate().getTime());
        writeStringLength256(buffer, pattern.getLanguage());
        buffer.writeInt(pattern.getCategory().ordinal());
        writeStringLength65536(buffer, pattern.getDescriptionPattern());
    }

    public static void writeVodDescriptorSet(ByteBuf buffer, Set<VodDescriptor> nodeDescriptors) throws MessageEncodingException {
        if (nodeDescriptors == null) {
            UserTypesEncoderFactory.writeUnsignedintAsTwoBytes(buffer, 0);
            return;
        }
        writeUnsignedintAsTwoBytes(buffer, nodeDescriptors.size());
        for (VodDescriptor node : nodeDescriptors) {
            writeVodNodeDescriptor(buffer, node);
        }
    }
}