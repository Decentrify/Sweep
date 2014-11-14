/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.ms.net;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.cm.events.ChunkedMessageFactory;
import se.sics.gvod.common.msgs.MessageDecodingException;
import se.sics.gvod.net.BaseMsgFrameDecoder;
import se.sics.gvod.net.msgs.RewriteableMsg;
import se.sics.ms.messages.*;

/**
 *
 * @author jdowling
 */
public class MessageFrameDecoder extends BaseMsgFrameDecoder {

    private static final Logger logger = LoggerFactory.getLogger(MessageFrameDecoder.class);
    // MEDIASEARCH MESSAGES
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
    public static final byte VOTING_RESULT_MESSAGE         = 0x6d;
    public static final byte REJECT_FOLLOWER_REQUEST       = 0x6e;
    public static final byte REJECT_FOLLOWER_RESPONSE      = 0x6f;
    public static final byte REJECT_LEADER_MESSAGE         = 0x70;
    public static final byte LEADER_LOOKUP_REQUEST         = 0x71;
    public static final byte LEADER_LOOKUP_RESPONSE        = 0x72;
    public static final byte REPAIR_REQUEST                = 0x73;
    public static final byte REPAIR_RESPONSE               = 0x74;
    public static final byte PUBLIC_KEY_MESSAGE            = 0x75;
    public static final byte PREPAIR_COMMIT_REQUEST        = 0x76;
    public static final byte PREPAIR_COMMIT_RESPONSE       = 0x77;
    public static final byte COMMIT_REQUEST                = 0x78;
    public static final byte COMMIT_RESPONSE               = 0x79;
    public static final byte INDEX_HASH_EXCHANGE_REQUEST   = 0x7a;
    public static final byte INDEX_HASH_EXCHANGE_RESPONSE  = 0x7b;
    //Two Phase Commit For Partitioning.
    public static final byte PARTITION_PREPARE_REQUEST     = 0x7c;
    public static final byte PARTITION_PREPARE_RESPONSE    = 0x7d;
    public static final byte PARTITION_COMMIT_REQUEST      = 0x7e;
    public static final byte PARTITION_COMMIT_RESPONSE     = 0x7f;

    // Control Message
    public static final byte CONTROL_MESSAGE_REQUEST = -0x01;
    public static final byte CONTROL_MESSAGE_RESPONSE = -0x02;

    // Delayed Partitioning Request
    public static final byte DELAYED_PARTITIONING_MESSAGE_REQUEST  = -0x03;
    public static final byte DELAYED_PARTITIONING_MESSAGE_RESPONSE  = -0x04;

    //Chunked message to fragment big messages
    public static final byte CHUNKED_MESSAGE  = -0x05;

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
            case INDEX_HASH_EXCHANGE_REQUEST:
                return IndexHashExchangeMessageFactory.Request.fromBuffer(buffer);
            case INDEX_HASH_EXCHANGE_RESPONSE:
                return IndexHashExchangeMessageFactory.Response.fromBuffer(buffer);

            // Two Phase Partitioning Commit.
            case PARTITION_PREPARE_REQUEST:
                return PartitionPrepareMessageFactory.Request.fromBuffer(buffer);
            case PARTITION_PREPARE_RESPONSE:
                return PartitionPrepareMessageFactory.Response.fromBuffer(buffer);
            case PARTITION_COMMIT_REQUEST:
                return PartitionCommitMessageFactory.Request.fromBuffer(buffer);
            case PARTITION_COMMIT_RESPONSE:
                return PartitionCommitMessageFactory.Response.fromBuffer(buffer);
            case CONTROL_MESSAGE_REQUEST:
                return ControlMessageFactory.Request.fromBuffer(buffer);
            case CONTROL_MESSAGE_RESPONSE:
                return ControlMessageFactory.Response.fromBuffer(buffer);

            // Delayed Partitioning Updates.
            case DELAYED_PARTITIONING_MESSAGE_REQUEST:
                return DelayedPartitioningMessageFactory.Request.fromBuffer(buffer);
            case DELAYED_PARTITIONING_MESSAGE_RESPONSE:
                return DelayedPartitioningMessageFactory.Response.fromBuffer(buffer);

            //Chunked message to fragment big messages
            case CHUNKED_MESSAGE:
                return ChunkedMessageFactory.fromBuffer(buffer);

            default:
                break;
        }

        return null;
    }
}
