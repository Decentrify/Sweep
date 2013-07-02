/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.peersearch.net;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.common.msgs.MessageDecodingException;
import se.sics.peersearch.messages.AddIndexEntryMessageFactory;
import se.sics.peersearch.messages.ReplicationMessageFactory;
import se.sics.peersearch.messages.SearchMessageFactory;
import se.sics.gvod.net.BaseMsgFrameDecoder;
import se.sics.gvod.net.msgs.RewriteableMsg;

/**
 *
 * @author jdowling
 */
public class MessageFrameDecoder extends BaseMsgFrameDecoder {

    private static final Logger logger = LoggerFactory.getLogger(MessageFrameDecoder.class);
    // PEERSEARCH MESSAGES
    public static final byte SEARCH_REQUEST                = 0x60;
    public static final byte SEARCH_RESPONSE               = 0x61;
    public static final byte ADD_ENTRY_REQUEST             = 0x62;
    public static final byte ADD_ENTRY_RESPONSE            = 0x63;
    public static final byte REPLICATION_REQUEST           = 0x64;
    public static final byte REPLICATION_RESPONSE          = 0x65;

    // NB: RANGE OF +VE BYTES ENDS AT 0x7F
    public MessageFrameDecoder() {
        super();
    }

    /**
     * Subclasses should call super() on their first line, and if a msg is
     * returned, then return, else test messages in this class.
     *
     * @param ctx
     * @param channel
     * @param buffer
     * @return
     * @throws MessageDecodingException
     */
    @Override
    protected RewriteableMsg decodeMsg(ChannelHandlerContext ctx,
            Channel channel, ChannelBuffer buffer) throws MessageDecodingException {
        
        // See if msg is part of parent project, if yes then return it.
        // Otherwise decode the msg here.
        RewriteableMsg msg = super.decodeMsg(ctx, channel, buffer);
        if (msg != null) {
            return msg;
        }
        switch (opKod) {
            case SEARCH_REQUEST:
                return SearchMessageFactory.Request.fromBuffer(buffer);
            case SEARCH_RESPONSE:
                return SearchMessageFactory.Response.fromBuffer(buffer);
            case ADD_ENTRY_REQUEST:
                return AddIndexEntryMessageFactory.Request.fromBuffer(buffer);
            case ADD_ENTRY_RESPONSE:
                return AddIndexEntryMessageFactory.Response.fromBuffer(buffer);
            case REPLICATION_REQUEST:
                return ReplicationMessageFactory.Request.fromBuffer(buffer);
            case REPLICATION_RESPONSE:
                return ReplicationMessageFactory.Response.fromBuffer(buffer);
            default:
                break;
        }

        return null;
    }
}
