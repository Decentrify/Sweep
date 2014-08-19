package se.sics.ms.gradient.control;

import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.msgs.MessageEncodingException;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.util.UserTypesEncoderFactory;
import se.sics.ms.net.ApplicationTypesEncoderFactory;
import se.sics.ms.util.PartitionHelper;

import java.util.LinkedList;

/**
 * Used for encoding the control messages in byte array.
 *
 * This class is similar to the ApplicationTypesEncoderFactory and UserTypesEncoderFactory.
 * Created by babbarshaer on 2014-07-31.
 */
public class ControlMessageEncoderFactory {

    /**
     * Encode the PartitionHashes in the ByteBuf passed.
     * @param buffer
     * @param controlMessageEnum
     * @param partitionUpdateHashes
     * @throws MessageEncodingException
     */
    private static void encodePartitioningUpdateHashesSequence(ByteBuf buffer, ControlMessageEnum controlMessageEnum , LinkedList<PartitionHelper.PartitionInfoHash> partitionUpdateHashes) throws MessageEncodingException {

        // Create an expandable buffer and encode the control message enum in it.
        buffer.writeInt(controlMessageEnum.ordinal());
        ApplicationTypesEncoderFactory.writePartitionUpdateHashSequence(buffer, partitionUpdateHashes);
    }

    private static void encodeLeaderUpdate(ByteBuf buffer, VodAddress leader) throws MessageEncodingException {

        buffer.writeInt(ControlMessageEnum.LEADER_UPDATE.ordinal());
        if(leader != null) {
            //to indicate that leader info is available in the encoded message
            buffer.writeBoolean(true);
            UserTypesEncoderFactory.writeVodAddress(buffer, leader);
        }
        else {
            buffer.writeBoolean(false);
        }
    }

    public static void encodeControlMessageInternal(ByteBuf buf, ControlMessageInternal.Response message) throws MessageEncodingException {

        if(message instanceof CheckPartitionInfoHashUpdate.Response) {
            CheckPartitionInfoHashUpdate.Response event = (CheckPartitionInfoHashUpdate.Response)message;
            ControlMessageEncoderFactory.encodePartitioningUpdateHashesSequence(buf, event.getControlMessageEnum() , event.getPartitionUpdateHashes());
        }
        else if(message instanceof CheckLeaderInfoUpdate.Response) {
            CheckLeaderInfoUpdate.Response event = (CheckLeaderInfoUpdate.Response)message;
            ControlMessageEncoderFactory.encodeLeaderUpdate(buf, event.getLeader());
        }
    }
}
