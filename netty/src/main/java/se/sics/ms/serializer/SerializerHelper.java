

package se.sics.ms.serializer;

import se.sics.ktoolbox.aggregator.AggregatorSerializerSetup;
import se.sics.ktoolbox.chunkmanager.ChunkManagerSerializerSetup;
import se.sics.ktoolbox.croupier.CroupierSerializerSetup;
import se.sics.ktoolbox.election.ElectionSerializerSetup;
import se.sics.ktoolbox.gradient.GradientSerializerSetup;
import se.sics.ktoolbox.util.setup.BasicSerializerSetup;
import se.sics.ms.net.SweepSerializerSetup;

/**
 * Helper class used for manipulating and upgrading the
 * serializers.
 *
 * Created by babbar on 2015-08-18.
 */
public class SerializerHelper {


    public static void registerSerializers(int startId){

        int currentId = startId;
        currentId = BasicSerializerSetup.registerBasicSerializers(currentId);
        currentId = CroupierSerializerSetup.registerSerializers(currentId);
        currentId = GradientSerializerSetup.registerSerializers(currentId);
        currentId = ElectionSerializerSetup.registerSerializers(currentId);
        currentId = AggregatorSerializerSetup.registerSerializers(currentId);
        currentId = ChunkManagerSerializerSetup.registerSerializers(currentId);
        SweepSerializerSetup.registerSerializers(currentId);
    }



}
