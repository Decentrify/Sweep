
package se.sics.ms.main;

import se.sics.ktoolbox.aggregator.server.event.AggregatedInfo;
import se.sics.ms.data.InternalStatePacket;
import se.sics.ms.helper.AggregatedInfoSimSerializer;
import se.sics.ms.helper.ISPSimSerializer;

/**
 * Setting up the serializers used in the simulation.
 *
 * Created by babbar on 2015-09-19.
 */
public class SimulationSerializerSetup {


    public static void registerSerializers(int id){

        int startId= id;

        ISPSimSerializer internalStatePacketSerializer = new ISPSimSerializer(startId++);
        SimulationSerializers.registerSerializer(InternalStatePacket.class, internalStatePacketSerializer);

        AggregatedInfoSimSerializer aggregatedInfoSerializer = new AggregatedInfoSimSerializer(startId++);
        SimulationSerializers.registerSerializer(AggregatedInfo.class, aggregatedInfoSerializer);

    }

}
