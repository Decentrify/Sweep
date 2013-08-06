/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.peersearch.net;

import io.netty.buffer.ByteBuf;
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
    public static final byte INDEX_EXCHANGE_REQUEST        = 0x64;
    public static final byte INDEX_EXCHANGE_RESPONSE       = 0x65;
    public static final byte LEADER_SELECTION_REQUEST      = 0x66;
    public static final byte LEADER_SELECTION_RESPONSE     = 0x67;
    public static final byte GRADIENT_SHUFFLE_REQUEST      = 0x68;
    public static final byte GRADIENT_SHUFFLE_RESPONSE     = 0x69;
    public static final byte LEADER_SUSPICION_REQUEST      = 0x6a;
    public static final byte LEADER_SUSPICION_RESPONSE     = 0x6b;
    public static final byte LEADER_ANNOUNCEMENT           = 0x6c;
    public static final byte HEARTBEAT_REQUEST             = 0x6d;
    public static final byte HEARTBEAT_RESPONSE            = 0x7e;
    public static final byte VOTING_RESULT_MESSAGE         = 0x7f;
    public static final byte REJECT_FOLLOWER_REQUEST       = 0x70;
    public static final byte REJECT_FOLLOWER_RESPONSE      = 0x71;
    public static final byte REJECT_LEADER_MESSAGE         = 0x72;
    public static final byte LEADER_LOOKUP_REQUEST         = 0x73;
    public static final byte LEADER_LOOKUP_RESPONSE        = 0x74;
    public static final byte REPAIR_REQUEST                = 0x75;
    public static final byte REPAIR_RESPONSE               = 0x76;
    public static final byte PUBLIC_KEY_MESSAGE            = 0x77;
    public static final byte PREPAIR_COMMIT_REQUEST        = 0x78;
    public static final byte PREPAIR_COMMIT_RESPONSE       = 0x79;
    public static final byte COMMIT_REQUEST                = 0x7a;
    public static final byte COMMIT_RESPONSE               = 0x7b;

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
            case INDEX_EXCHANGE_REQUEST:
                return IndexExchangeMessageFactory.Request.fromBuffer(buffer);
            case INDEX_EXCHANGE_RESPONSE:
                return IndexExchangeMessageFactory.Response.fromBuffer(buffer);
            case LEADER_SELECTION_REQUEST:
                return ElectionMessageFactory.Request.fromBuffer(buffer);
            case LEADER_SELECTION_RESPONSE:
                return ElectionMessageFactory.Response.fromBuffer(buffer);
            case GRADIENT_SHUFFLE_REQUEST:
                return GradientShuffleMessageFactory.Request.fromBuffer(buffer);
            case GRADIENT_SHUFFLE_RESPONSE:
                return GradientShuffleMessageFactory.Response.fromBuffer(buffer);
            case LEADER_SUSPICION_REQUEST:
                return LeaderSuspicionMessageFactory.Request.fromBuffer(buffer);
            case LEADER_SUSPICION_RESPONSE:
                return LeaderSuspicionMessageFactory.Response.fromBuffer(buffer);
            case LEADER_ANNOUNCEMENT:
                return LeaderDeathAnnouncementMessageFactory.fromBuffer(buffer);
            case HEARTBEAT_REQUEST:
                return HeartbeatMessageFactory.Request.fromBuffer(buffer);
            case HEARTBEAT_RESPONSE:
                return HeartbeatMessageFactory.Response.fromBuffer(buffer);
            case VOTING_RESULT_MESSAGE:
                return VotingResultMessageFactory.fromBuffer(buffer);
            case REJECT_FOLLOWER_REQUEST:
                return RejectFollowerMessageFactory.Request.fromBuffer(buffer);
            case REJECT_FOLLOWER_RESPONSE:
                return RejectFollowerMessageFactory.Response.fromBuffer(buffer);
            case REJECT_LEADER_MESSAGE:
                return RejectLeaderMessageFactory.fromBuffer(buffer);
            case LEADER_LOOKUP_REQUEST:
                return LeaderLookupMessageFactory.Request.fromBuffer(buffer);
            case LEADER_LOOKUP_RESPONSE:
                return LeaderLookupMessageFactory.Response.fromBuffer(buffer);
            case REPAIR_REQUEST:
                return RepairMessageFactory.Request.fromBuffer(buffer);
            case REPAIR_RESPONSE:
                return RepairMessageFactory.Response.fromBuffer(buffer);
            case PUBLIC_KEY_MESSAGE:
                return PublicKeyMessageFactory.fromBuffer(buffer);
            case PREPAIR_COMMIT_REQUEST:
                return ReplicationPrepairCommitMessageFactory.Request.fromBuffer(buffer);
            case PREPAIR_COMMIT_RESPONSE:
                return ReplicationPrepairCommitMessageFactory.Response.fromBuffer(buffer);
            case COMMIT_REQUEST:
                return ReplicationCommitMessageFactory.Request.fromBuffer(buffer);
            case COMMIT_RESPONSE:
                return ReplicationCommitMessageFactory.Response.fromBuffer(buffer);
            default:
                break;
        }

        return null;
    }
}
