package se.sics.ms.gradient;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import se.sics.gvod.common.msgs.MessageEncodingException;
import se.sics.gvod.net.util.UserTypesEncoderFactory;
import se.sics.ms.net.ApplicationTypesEncoderFactory;
import se.sics.ms.util.PartitionHelper;

import java.nio.ByteBuffer;
import java.util.LinkedList;

/**
 * Used for encoding the control messages in byte array.
 *
 * This class is similar to the ApplicationTypesEncoderFactory and UserTypesEncoderFactory.
 * Created by babbarshaer on 2014-07-31.
 */
public class ControlMessageEncoderFactory {


    public static void encodePartitioningUpdateSequence(ByteBuf buffer , ControlMessageEnum controlMessageEnum, LinkedList<PartitionHelper.PartitionInfo> partitionUpdates) throws MessageEncodingException {


            // Create an expandable buffer and encode the control message enum in it.
            buffer.writeInt(controlMessageEnum.ordinal());

            if(controlMessageEnum == ControlMessageEnum.REJOIN || controlMessageEnum == ControlMessageEnum.NO_PARTITION_UPDATE)
                UserTypesEncoderFactory.writeUnsignedintAsOneByte(buffer, 0);

            else {
                // Update the method and make it more uniform because of multiple check it is becoming a problem.
                // TODO: Improvement after Test Run.
                ApplicationTypesEncoderFactory.writeDelayedPartitionInfo(buffer, partitionUpdates);
            }

    }

    /**
     * Encode the PartitionHashes in the ByteBuf passed.
     * @param buffer
     * @param controlMessageEnum
     * @param partitionUpdateHashes
     * @throws MessageEncodingException
     */
    public static void encodePartitioningUpdateHashesSequence(ByteBuf buffer, ControlMessageEnum controlMessageEnum , LinkedList<PartitionHelper.PartitionInfoHash> partitionUpdateHashes) throws MessageEncodingException {

        // Create an expandable buffer and encode the control message enum in it.
        buffer.writeInt(controlMessageEnum.ordinal());
        ApplicationTypesEncoderFactory.writePartitionUpdateHashSequence(buffer, partitionUpdateHashes);
    }


}
