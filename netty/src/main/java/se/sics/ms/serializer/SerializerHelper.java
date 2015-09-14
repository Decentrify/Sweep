

package se.sics.ms.serializer;

import se.sics.ktoolbox.aggregator.global.network.AggregatorSerializerSetup;
import se.sics.ms.net.SerializerSetup;
import se.sics.p2ptoolbox.chunkmanager.ChunkManagerSerializerSetup;
import se.sics.p2ptoolbox.croupier.CroupierSerializerSetup;
import se.sics.p2ptoolbox.election.network.ElectionSerializerSetup;
import se.sics.p2ptoolbox.gradient.GradientSerializerSetup;
import se.sics.p2ptoolbox.util.serializer.BasicSerializerSetup;

/**
 * Helper class used for manipulating and upgrading the
 * serializers.
 *
 * Created by babbar on 2015-08-18.
 */
public class SerializerHelper {


    public static void registerSerializers(int startId){

        int currentId = startId;
        BasicSerializerSetup.registerBasicSerializers(currentId);
        currentId += BasicSerializerSetup.serializerIds;
        currentId = CroupierSerializerSetup.registerSerializers(currentId);
        currentId = GradientSerializerSetup.registerSerializers(currentId);
        currentId = ElectionSerializerSetup.registerSerializers(currentId);
        currentId = AggregatorSerializerSetup.registerSerializers(currentId);
        currentId = ChunkManagerSerializerSetup.registerSerializers(currentId);
        SerializerSetup.registerSerializers(currentId);
    }



}
