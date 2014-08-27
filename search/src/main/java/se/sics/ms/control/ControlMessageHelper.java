package se.sics.ms.control;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Helper Class for the Processing of the Control Message.
 *
 * Created by babbarshaer on 2014-08-03.
 */
public class ControlMessageHelper {
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
            listControl = new ArrayList<T>();
        listControl.add(controlResponse);

        controlMessageResponseHolderMap.put(controlResponse.getControlMessageResponseTypeEnum(), listControl);
    }
}
