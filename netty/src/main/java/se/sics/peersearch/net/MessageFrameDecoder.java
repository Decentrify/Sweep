/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.peersearch.net;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.common.msgs.MessageDecodingException;
import se.sics.peersearch.messages.*;
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
    public static final byte INDEX_EXCHANGE_REQUEST        = 0x66;
    public static final byte INDEX_EXCHANGE_RESPONSE       = 0x67;
    public static final byte GAP_DETECTION_REQUEST         = 0x68;
    public static final byte GAP_DETECTION_RESPONSE        = 0x69;
    public static final byte LEADER_SELECTION_REQUEST      = 0x6a;
    public static final byte LEADER_SELECTION_RESPONSE     = 0x6b;
    public static final byte GRADIENT_SHUFFLE_REQUEST      = 0x6c;
    public static final byte GRADIENT_SHUFFLE_RESPONSE     = 0x6d;
    public static final byte LEADER_SUSPECTION_REQUEST     = 0x6e;
    public static final byte LEADER_SUSPECTION_RESPONSE    = 0x6f;
    public static final byte LEADER_ANNOUNCEMENT           = 0x70;
    public static final byte HEARTBEAT_REQUEST             = 0x71;
    public static final byte HEARTBEAT_RESPONSE            = 0x72;
    public static final byte ADD_INDEX_ENTRY_ROUTED        = 0x73;
    public static final byte GAP_DETECTION_ROUTED          = 0x74;
    public static final byte VOTING_RESULT_MESSAGE         = 0x75;
    public static final byte REJECT_FOLLOWER_REQUEST       = 0x76;
    public static final byte REJECT_FOLLOWER_RESPONSE      = 0x77;
    public static final byte REJECT_LEADER_MESSAGE         = 0x78;
    public static final byte START_INDEX_REQUEST_MESSAGE   = 0x79;
    public static final byte INDEX_REQUEST_MESSAGE         = 0x7a;
    public static final byte INDEX_DESSIMINATION_MESSAGE   = 0x7b;
    public static final byte INDEX_RESPONSE_MESSAGE        = 0x7c;

    // NB: RANGE OF +VE BYTES ENDS AT 0x7F
    public MessageFrameDecoder() {
        super();
    }

    /**
     * Subclasses should call super() on their first line, and if a msg is
     * returned, then return, else test messages in this class.
     *
     * @param ctx
     * @param buffer
     * @return
     * @throws MessageDecodingException
     */
    @Override
    protected RewriteableMsg decodeMsg(ChannelHandlerContext ctx,
            ByteBuf buffer) throws MessageDecodingException {
        
        // See if msg is part of parent project, if yes then return it.
        // Otherwise decode the msg here.
        RewriteableMsg msg = super.decodeMsg(ctx, buffer);
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
            case INDEX_EXCHANGE_REQUEST:
                return IndexExchangeMessageFactory.Request.fromBuffer(buffer);
            case INDEX_EXCHANGE_RESPONSE:
                return IndexExchangeMessageFactory.Response.fromBuffer(buffer);
            case GAP_DETECTION_REQUEST:
                return GapDetectionMessageFactory.Request.fromBuffer(buffer);
            case GAP_DETECTION_RESPONSE:
                return GapDetectionMessageFactory.Response.fromBuffer(buffer);
            case LEADER_SELECTION_REQUEST:
                return ElectionMessageFactory.Request.fromBuffer(buffer);
            case LEADER_SELECTION_RESPONSE:
                return ElectionMessageFactory.Response.fromBuffer(buffer);
            case GRADIENT_SHUFFLE_REQUEST:
                return GradientShuffleMessageFactory.Request.fromBuffer(buffer);
            case GRADIENT_SHUFFLE_RESPONSE:
                return GradientShuffleMessageFactory.Response.fromBuffer(buffer);
            case LEADER_SUSPECTION_REQUEST:
                return LeaderSuspectionMessageFactory.Request.fromBuffer(buffer);
            case LEADER_SUSPECTION_RESPONSE:
                return LeaderSuspectionMessageFactory.Response.fromBuffer(buffer);
            case LEADER_ANNOUNCEMENT:
                return LeaderAnnouncementMessageFactory.fromBuffer(buffer);
            case HEARTBEAT_REQUEST:
                return HeartbeatMessageFactory.Request.fromBuffer(buffer);
            case HEARTBEAT_RESPONSE:
                return HeartbeatMessageFactory.Response.fromBuffer(buffer);
            case ADD_INDEX_ENTRY_ROUTED:
                return AddIndexEntryRoutedMessageFactory.fromBuffer(buffer);
            case GAP_DETECTION_ROUTED:
                return GapDetectionRoutedMessageFactory.fromBuffer(buffer);
            case VOTING_RESULT_MESSAGE:
                return VotingResultMessageFactory.fromBuffer(buffer);
            case REJECT_FOLLOWER_REQUEST:
                return RejectFollowerMessageFactory.Request.fromBuffer(buffer);
            case REJECT_FOLLOWER_RESPONSE:
                return RejectFollowerMessageFactory.Response.fromBuffer(buffer);
            case REJECT_LEADER_MESSAGE:
                return RejectLeaderMessageFactory.fromBuffer(buffer);
            case START_INDEX_REQUEST_MESSAGE:
//                return StartIndexRequestMessageFactory.fromBuffer(buffer);
            case INDEX_REQUEST_MESSAGE:
                return IndexRequestMessageFactory.fromBuffer(buffer);
            case INDEX_DESSIMINATION_MESSAGE:
                return IndexDisseminationMessageFactory.fromBuffer(buffer);
            case INDEX_RESPONSE_MESSAGE:
                return  IndexResponseMessageFactory.fromBuffer(buffer);
            default:
                break;
        }

        return null;
    }
}
