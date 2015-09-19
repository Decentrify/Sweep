
package se.sics.ms.main;

import java.nio.ByteBuffer;

/**
 * Main class for the serialization process during the
 * simulation.
 *
 * Created by babbar on 2015-09-19.
 */
public interface SimulationSerializer {


    /**
     * The identifier representing the serializer
     * to be used or used for the object in the system.
     *
     * @return
     */
    public int getIdentifier();


    /**
     * Get size of the object in terms of bytes
     * that object will occupy after serialization.
     * @return
     */
    public int getByteSize(Object baseObject);


    /**
     * Convert the object in binary array when executing this
     * method.
     *
     * @param o
     * @param buffer
     */
    public void toBinary(Object o, ByteBuffer buffer);


    /**
     * Construct the object from the byte buffer.
     *
     * @param buffer buffer
     * @return Object.
     */
    public Object fromBinary(ByteBuffer buffer);


}
