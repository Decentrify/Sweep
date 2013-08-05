/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.peersearch.net;

import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.VodDescriptor;
import se.sics.gvod.common.msgs.MessageDecodingException;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.util.UserTypesDecoderFactory;
import se.sics.peersearch.types.IndexEntry;
import se.sics.peersearch.types.SearchPattern;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import static se.sics.gvod.net.util.UserTypesDecoderFactory.readGVodNodeDescriptor;

/**
 *
 * @author jdowling
 */
public class ApplicationTypesDecoderFactory {

    public static IndexEntry readIndexEntry(ByteBuf buffer)
            throws MessageDecodingException {
        Long id = buffer.readLong();
        String url = UserTypesDecoderFactory.readStringLength256(buffer);
        String fileName = UserTypesDecoderFactory.readStringLength256(buffer);
        Long fileSize = buffer.readLong();
        Date uploaded =  new Date(buffer.readLong());
        String language = UserTypesDecoderFactory.readStringLength256(buffer);
        IndexEntry.Category category = IndexEntry.Category.values()[buffer.readInt()];
        String description = UserTypesDecoderFactory.readStringLength65536(buffer);
        String hash = UserTypesDecoderFactory.readStringLength256(buffer);
        String leaderId = UserTypesDecoderFactory.readStringLength256(buffer);

        return new IndexEntry(id, url, fileName, fileSize, uploaded, language, category, description, hash, leaderId);
    }

    public static IndexEntry[] readIndexEntryArray(ByteBuf buffer) throws MessageDecodingException {
        int len = UserTypesDecoderFactory.readUnsignedIntAsTwoBytes(buffer);
        IndexEntry[] items = new IndexEntry[len];
        for (int i = 0; i < len; i++) {
            items[i] = readIndexEntry(buffer);
        }
        return items;
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
        IndexEntry.Category category = IndexEntry.Category.values()[buffer.readInt()];
        String descriptionPattern = UserTypesDecoderFactory.readStringLength65536(buffer);

        return new SearchPattern(fileNamePattern, minFileSize, maxFileSize, minUploadDate, maxUploadDate, language, category, descriptionPattern);
    }

    public static Set<VodDescriptor> readVodDescriptorSet(ByteBuf buffer) throws MessageDecodingException {
        int len = UserTypesDecoderFactory.readUnsignedIntAsTwoBytes(buffer);
        Set<VodDescriptor> addrs = new HashSet<VodDescriptor>();
        for (int i = 0; i < len; i++) {
            addrs.add(readGVodNodeDescriptor(buffer));
        }
        return addrs;
    }
}
