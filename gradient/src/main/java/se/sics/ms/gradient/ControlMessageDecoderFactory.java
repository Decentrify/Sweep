package se.sics.ms.gradient;

import io.netty.buffer.ByteBuf;
import se.sics.gvod.net.util.UserTypesDecoderFactory;

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


}
