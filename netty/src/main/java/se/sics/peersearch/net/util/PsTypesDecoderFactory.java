/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.peersearch.net.util;

import org.jboss.netty.buffer.ChannelBuffer;
import se.sics.gvod.common.msgs.MessageDecodingException;

/**
 *
 * @author jdowling
 */
public class PsTypesDecoderFactory {

    public static Integer readEncodedSubPiece(ChannelBuffer buffer) throws MessageDecodingException {
        int globalId = buffer.readInt();
        return globalId;
    }
}
