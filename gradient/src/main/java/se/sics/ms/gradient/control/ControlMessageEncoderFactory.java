package se.sics.ms.gradient.control;

import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.msgs.MessageEncodingException;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.util.UserTypesEncoderFactory;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.ms.net.ApplicationTypesEncoderFactory;
import se.sics.ms.util.PartitionHelper;
import se.sics.p2ptoolbox.util.network.impl.DecoratedAddress;

import java.security.PublicKey;
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
        
        if(partitionUpdateHashes == null){
            buffer.writeInt(0);
            partitionUpdateHashes = new LinkedList<PartitionHelper.PartitionInfoHash>();
        }
        else{
            buffer.writeInt(partitionUpdateHashes.size());
        }
        
        for(PartitionHelper.PartitionInfoHash infoHash : partitionUpdateHashes){
            Serializers.lookupSerializer(PartitionHelper.PartitionInfoHash.class).toBinary(infoHash, buffer);
        }
    }

    private static void encodeLeaderUpdate(ByteBuf buffer, DecoratedAddress leader, PublicKey leaderPublicKey) throws MessageEncodingException {

        buffer.writeInt(ControlMessageEnum.LEADER_UPDATE.ordinal());
        if(leader != null) {
            //to indicate that leader info is available in the encoded message
            buffer.writeBoolean(true);
            Serializers.lookupSerializer(DecoratedAddress.class).toBinary(leader, buffer);
            Serializers.lookupSerializer(PublicKey.class).toBinary(leaderPublicKey, buffer);
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
            ControlMessageEncoderFactory.encodeLeaderUpdate(buf, event.getLeader(), event.getLeaderPublicKey());
        }
    }
}
