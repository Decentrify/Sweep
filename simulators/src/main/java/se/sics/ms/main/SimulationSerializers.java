package se.sics.ms.main;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Serializers used for the simulation.
 *
 * Created by babbar on 2015-09-19.
 */
public class SimulationSerializers {

    private static Map<Class, SimulationSerializer> serializerMap = new HashMap<Class, SimulationSerializer>();
    private static Map<Integer, SimulationSerializer> serializerIdentifierMap = new HashMap<Integer, SimulationSerializer>();

    /**
     * Register the serializer by storing the reference
     * in the container map which will be used during serialization.
     *
     * @param serializedObjClass   serializedObject
     * @param serializer serializer used.
     */
    public static void registerSerializer(Class serializedObjClass, SimulationSerializer serializer){

        serializerMap.put(serializedObjClass, serializer);
        serializerIdentifierMap.put(serializer.getIdentifier(), serializer);
    }

    /**
     * In case the application knows which serializer to use, then the
     * application can by default use the serializer.
     *
     * @param objectClass object class
     * @return
     */
    public static SimulationSerializer lookupSerializer(Class objectClass){
        return serializerMap.get(objectClass);
    }


    /**
     * In case serializer is unknown, the application
     * can simply let the Serializers to handle the situation
     * by determining the serializer from the list.
     *
     * A marker needs to be placed before the serializer to identify the serializer used
     * during the deserialization.
     *
     * @param o
     * @param buffer
     */
    public static void toBinary(Object o, ByteBuffer buffer){

        SimulationSerializer serializer = serializerMap.get(o.getClass());
        if(serializer == null){
            throw new RuntimeException("Unable to locate serializer when trying to convert to binary.");
        }

        buffer.putInt(serializer.getIdentifier());
        serializer.toBinary(o, buffer);
    }


    /**
     * Identifier of the serializer is read and used to locate the
     * serializer which will carry forward the deserialization.
     *
     * @param buffer buffer
     * @return Object
     */
    public static Object fromBinary(ByteBuffer buffer){


        int serializerId = buffer.getInt();
        SimulationSerializer serializer = serializerIdentifierMap.get(serializerId);

        if(serializer == null){
            throw new RuntimeException("Unable to locate the serializer during deserialization.");
        }

        return serializer.fromBinary(buffer);
    }

}
