/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.peersearch.net;

import org.jboss.netty.buffer.ChannelBuffer;
import se.sics.gvod.common.msgs.MessageDecodingException;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.util.UserTypesDecoderFactory;
import se.sics.peersearch.types.IndexEntry;

import java.util.Date;

/**
 *
 * @author jdowling
 */
public class ApplicationTypesDecoderFactory {

    public static IndexEntry readIndexEntry(ChannelBuffer buffer)
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

    public static IndexEntry[] readIndexEntryArray(ChannelBuffer buffer) throws MessageDecodingException {
        int len = UserTypesDecoderFactory.readUnsignedIntAsTwoBytes(buffer);
        IndexEntry[] items = new IndexEntry[len];
        for (int i = 0; i < len; i++) {
            items[i] = readIndexEntry(buffer);
        }
        return items;
    }

    public static Long[] readLongArray(ChannelBuffer buffer) {
        int len = UserTypesDecoderFactory.readUnsignedIntAsTwoBytes(buffer);
        Long[] items = new Long[len];
        for (int i = 0; i < len; i++) {
            items[i] = buffer.readLong();
        }
        return items;
    }

    public static VodAddress[] readVodAddressArray(ChannelBuffer buffer) throws MessageDecodingException {
        int len = UserTypesDecoderFactory.readUnsignedIntAsTwoBytes(buffer);
        VodAddress[] items = new VodAddress[len];
        for (int i = 0; i < len; i++) {
            items[i] = UserTypesDecoderFactory.readVodAddress(buffer);
        }
        return items;
    }
}
