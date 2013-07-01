/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.peersearch.net.util;

import org.jboss.netty.buffer.ChannelBuffer;
import se.sics.gvod.common.msgs.MessageEncodingException;

/**
 *
 * @author jdowling
 */
public class PsTypesEncoderFactory {
    
    public static void writeDmitryType(ChannelBuffer buffer, Integer id) throws MessageEncodingException {
        buffer.writeInt(id);
//        UserTypesEncoderFactory.writeAddress(buffer, null);
    }

    
    
}
