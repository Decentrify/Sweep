package se.sics.ms.control;

import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.msgs.MessageDecodingException;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.util.UserTypesDecoderFactory;
import se.sics.ms.gradient.control.ControlMessageEnum;
import se.sics.ms.messages.ControlMessage;
import se.sics.ms.net.ApplicationTypesDecoderFactory;
import se.sics.ms.util.PartitionHelper;

import java.util.List;

/**
 * Used for decoding the byte array back to object.
 *
 * Created by babbarshaer on 2014-07-31.
 */

public class ControlMessageDecoderFactory {


    /**
     * Return the control message enum.
     * @param buffer
     * @return
     */
    public static ControlMessageEnum getControlMessageEnum(ByteBuf buffer){

        int i = buffer.readInt();
        return ControlMessageEnum.values()[i];
    }

    /**
     * Return the number of updates.
     * @param buffer
     * @return
     */
    public static int getNumberOfUpdates(ByteBuf buffer){
        return  buffer.readInt();
    }


    private static ControlBase decodeLeaderUpdate(ByteBuf buffer) throws MessageDecodingException {

        boolean hasLeaderInfo = UserTypesDecoderFactory.readBoolean(buffer);

        if(hasLeaderInfo) {
            VodAddress leaderAddress = UserTypesDecoderFactory.readVodAddress(buffer);
            return new LeaderInfoControlResponse(leaderAddress);
        }

        return new LeaderInfoControlResponse(null);
    }

    private static ControlBase decodePartitioningHashUpdate(ControlMessageEnum controlMessageEnum,
                                                 VodAddress sourceAddress,
                                                 ByteBuf buffer) throws MessageDecodingException {

        // Fetch the list of partition hashes from the application decoder factory.
        List<PartitionHelper.PartitionInfoHash> partitionUpdateHashes;
        partitionUpdateHashes = ApplicationTypesDecoderFactory.readPartitionUpdateHashSequence(buffer);

        // Create a specific object.
        return new PartitionControlResponse(controlMessageEnum, partitionUpdateHashes, sourceAddress);
    }

    public static ControlBase decodeControlMessageInternal(ByteBuf buffer, ControlMessage.Response event) throws MessageDecodingException {

        // Read the Control Message Enum from the message.
        ControlMessageEnum controlMessageEnum = ControlMessageDecoderFactory.getControlMessageEnum(buffer);

        // Based on the control message enum received, update the control response map.
        switch(controlMessageEnum){

            case NO_PARTITION_UPDATE:
                return ControlMessageDecoderFactory.decodePartitioningHashUpdate(
                        ControlMessageEnum.NO_PARTITION_UPDATE, event.getVodSource(),
                        buffer);

            case PARTITION_UPDATE:
                return ControlMessageDecoderFactory.decodePartitioningHashUpdate(
                        ControlMessageEnum.PARTITION_UPDATE, event.getVodSource(),
                        buffer);

            case REJOIN:
                return ControlMessageDecoderFactory.decodePartitioningHashUpdate(
                        ControlMessageEnum.REJOIN, event.getVodSource(),
                        buffer);

            case LEADER_UPDATE:
                return ControlMessageDecoderFactory.decodeLeaderUpdate(buffer);

            default:
                return null;
        }
    }
}
