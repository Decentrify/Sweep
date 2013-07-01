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
import se.sics.peersearch.msgs.SearchMsgFactory;
import se.sics.gvod.net.BaseMsgFrameDecoder;
import se.sics.gvod.net.msgs.RewriteableMsg;

/**
 *
 * @author jdowling
 */
public class PsMsgFrameDecoder extends BaseMsgFrameDecoder {

    private static final Logger logger = LoggerFactory.getLogger(PsMsgFrameDecoder.class);
    // PEERSEARCH MESSAGES
    public static final byte SEARCH_REQUEST                = 0x60;
    public static final byte SEARCH_RESPONSE               = 0x62;

    // NB: RANGE OF +VE BYTES ENDS AT 0x7F
    public PsMsgFrameDecoder() {
        super();
    }

    /**
     * Subclasses should call super() on their first line, and if a msg is
     * returned, then return, else test msgs in this class.
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
                return SearchMsgFactory.Request.fromBuffer(buffer);
            case SEARCH_RESPONSE:
                return SearchMsgFactory.Response.fromBuffer(buffer);
            default:
                break;
        }

        return null;
    }
}
