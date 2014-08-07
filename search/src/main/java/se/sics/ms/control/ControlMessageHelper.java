package se.sics.ms.control;

import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.msgs.MessageDecodingException;
import se.sics.gvod.net.VodAddress;
import se.sics.ms.gradient.ControlMessageEnum;
import se.sics.ms.net.ApplicationTypesDecoderFactory;
import se.sics.ms.util.PartitionHelper;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Helper Class for the Processing of the Control Message.
 *
 * Created by babbarshaer on 2014-08-03.
 */
public class ControlMessageHelper {



    public static void addPartitioningHashUpdate(ControlMessageResponseTypeEnum controlMessageResponseTypeEnum, ControlMessageEnum controlMessageEnum , VodAddress sourceAddress , ByteBuf buffer, Map<ControlMessageResponseTypeEnum, List<? extends ControlBase>> controlMessageResponseHolderMap) throws MessageDecodingException {

        // Fetch the list of partition hashes from the application decoder factory.
        List<PartitionHelper.PartitionInfoHash> partitionUpdateHashes;
        partitionUpdateHashes = ApplicationTypesDecoderFactory.readPartitionUpdateHashSequence(buffer);

        // Create a specific object.
        PartitionControlResponse partitionControl = new PartitionControlResponse(controlMessageResponseTypeEnum, controlMessageEnum, partitionUpdateHashes, sourceAddress);

        // Update the list of objects in the final map.
        updateTheControlMessageResponseHolderMap(partitionControl, controlMessageResponseHolderMap);
    }


    /**
     * A generic method to simply update the control response holder map with appropriate object.
     *
     * @param controlResponse
     * @param controlMessageResponseHolderMap
     * @param <T>
     */
    public static <T extends ControlBase> void updateTheControlMessageResponseHolderMap ( T controlResponse, Map<ControlMessageResponseTypeEnum, List<? extends ControlBase>> controlMessageResponseHolderMap){

        List<T> listControl = (List<T>)controlMessageResponseHolderMap.get(controlResponse.getControlMessageResponseTypeEnum());
        if(listControl == null)
            listControl = new ArrayList<>();
        listControl.add(controlResponse);

        controlMessageResponseHolderMap.put(controlResponse.getControlMessageResponseTypeEnum(), listControl);
    }



}
