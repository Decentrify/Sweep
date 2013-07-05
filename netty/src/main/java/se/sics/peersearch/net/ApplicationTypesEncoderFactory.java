/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.peersearch.net;

import org.jboss.netty.buffer.ChannelBuffer;
import se.sics.gvod.common.msgs.MessageEncodingException;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.util.UserTypesEncoderFactory;
import se.sics.peersearch.types.IndexEntry;
import se.sics.peersearch.types.SearchPattern;

import static se.sics.gvod.net.util.UserTypesEncoderFactory.*;

/**
 *
 * @author jdowling
 */
public class ApplicationTypesEncoderFactory {
    
    public static void writeIndexEntry(ChannelBuffer buffer, IndexEntry indexEntry)
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
        writeStringLength256(buffer, indexEntry.getLeaderId());
    }

    public static void writeIndexEntryArray(ChannelBuffer buffer, IndexEntry[] items) throws MessageEncodingException {
        if(items == null){
            writeUnsignedintAsOneByte(buffer, 0);
            return;
        }
        UserTypesEncoderFactory.writeUnsignedintAsTwoBytes(buffer, items.length);
        for(IndexEntry item : items)
            writeIndexEntry(buffer, item);
    }

    public static void writeLongArray(ChannelBuffer buffer, Long[] items) throws MessageEncodingException {
        if (items == null) {
            writeUnsignedintAsOneByte(buffer, 0);
            return;
        }
        UserTypesEncoderFactory.writeUnsignedintAsTwoBytes(buffer, items.length);
        for (Long item : items)
            buffer.writeLong(item);
    }

    public static void writeVodAddressArray(ChannelBuffer buffer, VodAddress[] items) throws MessageEncodingException {
        if(items == null) {
            writeUnsignedintAsOneByte(buffer, 0);
            return;
        }

        UserTypesEncoderFactory.writeUnsignedintAsTwoBytes(buffer, items.length);
        for(VodAddress item : items)
            writeVodAddress(buffer, item);
    }

    public static void writeSearchPattern(ChannelBuffer buffer, SearchPattern pattern) throws MessageEncodingException {
        writeStringLength256(buffer, pattern.getFileNamePattern());
        buffer.writeInt(pattern.getMinFileSize());
        buffer.writeInt(pattern.getMaxFileSize());
        buffer.writeLong(pattern.getMinUploadDate().getTime());
        buffer.writeLong(pattern.getMaxUploadDate().getTime());
        writeStringLength256(buffer, pattern.getLanguage());
        buffer.writeInt(pattern.getCategory().ordinal());
        writeStringLength65536(buffer, pattern.getDescriptionPattern());
    }
}
