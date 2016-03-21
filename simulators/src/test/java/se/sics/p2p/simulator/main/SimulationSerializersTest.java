package se.sics.p2p.simulator.main;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.ms.main.SimulationSerializer;
import se.sics.ms.main.SimulationSerializers;

import java.nio.ByteBuffer;

/**
 * Created by babbarshaer on 2015-09-20.
 */
public class SimulationSerializersTest {

    public static Logger logger = LoggerFactory.getLogger(SimulationSerializersTest.class);

    public static void main(String[] args) {

        logger.debug("Going to initiate the process.");
        SimulationSerializers.registerSerializer(TestObject.class, new TestObjectSerializer(0));

        TestObject serializableObject = new TestObject(10, 10);
        SimulationSerializer serializer = SimulationSerializers.lookupSerializer(TestObject.class);

        int totalMemory = serializer.getByteSize(serializableObject);
        ByteBuffer buffer = ByteBuffer.allocate(totalMemory);

        serializer.toBinary(serializableObject, buffer);


        ByteBuffer newBuffer = ByteBuffer.wrap(buffer.array());
        TestObject deserializedObject = (TestObject) serializer.fromBinary(newBuffer);

        Assert.assertEquals(serializableObject, deserializedObject);
    }


    private static class TestObject {

        public Integer variable1;
        public Integer variable2;


        public TestObject(Integer var1, Integer var2){
            this.variable1 = var1;
            this.variable2 = var2;
        }

        @Override
        public String toString() {
            return "TestObject{" +
                    "variable1=" + variable1 +
                    ", variable2=" + variable2 +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TestObject that = (TestObject) o;

            if (variable1 != null ? !variable1.equals(that.variable1) : that.variable1 != null) return false;
            return !(variable2 != null ? !variable2.equals(that.variable2) : that.variable2 != null);

        }

        @Override
        public int hashCode() {
            int result = variable1 != null ? variable1.hashCode() : 0;
            result = 31 * result + (variable2 != null ? variable2.hashCode() : 0);
            return result;
        }
    }



    private static class TestObjectSerializer implements SimulationSerializer{

        private int id;


        public TestObjectSerializer(int id){
            this.id = id;
        }

        public int getIdentifier() {
            return this.id;
        }

        public int getByteSize(Object baseObject) {

            TestObject testObject = (TestObject) baseObject;
            int result = 0;
            result += 2*4;

            return result;
        }

        public void toBinary(Object o, ByteBuffer buffer) {
            TestObject testObject = (TestObject)o;
            buffer.putInt(testObject.variable1);
            buffer.putInt(testObject.variable2);
        }

        public Object fromBinary(ByteBuffer buffer) {

            Integer var1 = buffer.getInt();
            Integer var2 = buffer.getInt();

            return new TestObject(var1, var2);
        }
    }

}
