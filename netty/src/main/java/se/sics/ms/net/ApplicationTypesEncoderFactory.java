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
import se.sics.gvod.timer.TimeoutId;
import se.sics.ms.types.Id;
import se.sics.ms.types.IndexEntry;
import se.sics.ms.types.IndexHash;
import se.sics.ms.types.SearchPattern;
import se.sics.ms.util.PartitionHelper;
import sun.misc.BASE64Encoder;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
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

    public static void writeIndexEntryCollection(ByteBuf buffer, Collection<IndexEntry> items) throws MessageEncodingException {
        if(items == null){
            writeUnsignedintAsOneByte(buffer, 0);
            return;
        }
        UserTypesEncoderFactory.writeUnsignedintAsTwoBytes(buffer, items.size());
        for(IndexEntry item : items)
            writeIndexEntry(buffer, item);
    }

    public static void writeIndexEntryHash(ByteBuf buffer, IndexHash hash) throws MessageEncodingException {
        writeId(buffer, hash.getId());
        writeStringLength256(buffer, hash.getHash());
    }

    public static void writeIndexEntryHashCollection(ByteBuf buffer, Collection<IndexHash> hashes) throws MessageEncodingException {
        if (hashes == null) {
            UserTypesEncoderFactory.writeUnsignedintAsTwoBytes(buffer, 0);
            return;
        }

        UserTypesEncoderFactory.writeUnsignedintAsTwoBytes(buffer, hashes.size());
        for (IndexHash hash : hashes) {
            writeIndexEntryHash(buffer, hash);
        }
    }

    public static void writeId(ByteBuf buffer, Id id) throws MessageEncodingException {
        buffer.writeLong(id.getId());
        if(id.getLeaderId() == null)
            writeStringLength65536(buffer, new String());
        else
            writeStringLength65536(buffer, new BASE64Encoder().encode(id.getLeaderId().getEncoded()));
    }

    public static void writeIdCollection(ByteBuf buffer, Collection<Id> ids) throws MessageEncodingException {
        if(ids == null){
            writeUnsignedintAsOneByte(buffer, 0);
            return;
        }
        UserTypesEncoderFactory.writeUnsignedintAsTwoBytes(buffer, ids.size());
        for(Id id : ids)
            writeId(buffer, id);
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


    /**
     * Encoding for the partitioning update sequence.
     * @param buffer
     * @param partitionUpdatesSequence
     * @throws MessageEncodingException
     */
    public static void writeDelayedPartitionInfo(ByteBuf buffer, LinkedList<PartitionHelper.PartitionInfo> partitionUpdatesSequence) throws MessageEncodingException {

        if(partitionUpdatesSequence == null){
            writeUnsignedintAsTwoBytes(buffer,0);
            return;
        }
        writeUnsignedintAsTwoBytes(buffer,partitionUpdatesSequence.size());
        for(PartitionHelper.PartitionInfo partitionUpdate:  partitionUpdatesSequence){
            writePartitionUpdate(buffer, partitionUpdate);
        }

    }

    /**
     * Encoding required for writing partitioning update to the buffer.
     * @param buffer
     * @param partitionUpdate
     * @throws MessageEncodingException
     */
    public static void writePartitionUpdate(ByteBuf buffer, PartitionHelper.PartitionInfo partitionUpdate) throws MessageEncodingException {

        buffer.writeLong(partitionUpdate.getMedianId());
        UserTypesEncoderFactory.writeTimeoutId(buffer, partitionUpdate.getRequestId());
        buffer.writeInt(partitionUpdate.getPartitioningTypeInfo().ordinal());

        // Added support for the hash string of the Partition Update.
        writeStringLength256(buffer, partitionUpdate.getHash());
        if(partitionUpdate.getKey() == null)
            writeStringLength65536(buffer, new String());
        else
            writeStringLength65536(buffer, new BASE64Encoder().encode(partitionUpdate.getKey().getEncoded()));
    }


    public static void writePartitionRequestIds(ByteBuf buffer, List<TimeoutId> partitionRequestIds) throws MessageEncodingException {

        if(partitionRequestIds == null){
            UserTypesEncoderFactory.writeUnsignedintAsTwoBytes(buffer, 0);
            return;
        }

        UserTypesEncoderFactory.writeUnsignedintAsTwoBytes(buffer, partitionRequestIds.size());
        for(TimeoutId partitionRequestId : partitionRequestIds)
            UserTypesEncoderFactory.writeTimeoutId(buffer,partitionRequestId);
    }



    /**
     * Encode the PartitionUpdateHashSequence.
     * @param buffer
     * @param partitionUpdatesHash
     * @throws MessageEncodingException
     */

    public static void writePartitionUpdateHashSequence(ByteBuf buffer, LinkedList<PartitionHelper.PartitionInfoHash> partitionUpdatesHash) throws MessageEncodingException {

        if(partitionUpdatesHash == null){
            writeUnsignedintAsTwoBytes(buffer,0);
            return;
        }
        writeUnsignedintAsTwoBytes(buffer,partitionUpdatesHash.size());

        for(PartitionHelper.PartitionInfoHash partitionUpdateHash:  partitionUpdatesHash){
            writePartitionUpdateHash(buffer, partitionUpdateHash);
        }

    }

    /**
     * Encode the PartitionInfoHash Object.
     * @param buffer
     * @param partitionUpdateHash
     */
    private static void writePartitionUpdateHash(ByteBuf buffer, PartitionHelper.PartitionInfoHash partitionUpdateHash) throws MessageEncodingException {

        UserTypesEncoderFactory.writeTimeoutId(buffer, partitionUpdateHash.getPartitionRequestId());
        UserTypesEncoderFactory.writeStringLength65536(buffer, partitionUpdateHash.getHash());
    }


}
