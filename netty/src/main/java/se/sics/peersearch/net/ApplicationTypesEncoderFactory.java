/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.peersearch.net;

import org.jboss.netty.buffer.ChannelBuffer;
import se.sics.gvod.common.msgs.MessageEncodingException;
import se.sics.peersearch.types.IndexEntry;

import static se.sics.gvod.net.util.UserTypesEncoderFactory.writeStringLength256;
import static se.sics.gvod.net.util.UserTypesEncoderFactory.writeStringLength65536;

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
}
