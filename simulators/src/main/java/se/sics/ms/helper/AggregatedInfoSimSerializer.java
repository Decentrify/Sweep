package se.sics.ms.helper;

import se.sics.ktoolbox.aggregator.server.event.AggregatedInfo;
import se.sics.ktoolbox.aggregator.util.PacketInfo;
import se.sics.ms.main.SimulationSerializer;
import se.sics.ms.main.SimulationSerializers;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Simulation Serializer for the Aggregated information
 * which contains the aggregated information from all the nodes in the system.
 *
 * Created by babbarshaer on 2015-09-19.
 */
public class AggregatedInfoSimSerializer implements SimulationSerializer{


    private int identifier;

    public AggregatedInfoSimSerializer(int identifier){

        this.identifier = identifier;
    }

    public int getIdentifier() {
        return this.identifier;
    }

    public int getByteSize(Object baseObject) {

        AggregatedInfo aggregatedInfo = (AggregatedInfo)baseObject;
        Map<Integer, List<PacketInfo>> nodePacketMap = aggregatedInfo.getNodePacketMap();

        int result = 0;

        result += 8;    // For the timestamp.
        result += 4;    // For writing the size of the packet map.
        for(Map.Entry<Integer, List<PacketInfo>> entry : nodePacketMap.entrySet()){

            result += 4;    // For the Key.
            result += 4;    // For the value length.

            for(PacketInfo packet : entry.getValue()){

                result +=4; // For the serializer identifier.
                result += SimulationSerializers.lookupSerializer(packet.getClass()).getByteSize(packet);  // size for the particular type of packet.
            }
        }

        return result;
    }




    public void toBinary(Object o, ByteBuffer buffer) {

        AggregatedInfo aggregatedInfo = (AggregatedInfo)o;
        Map<Integer, List<PacketInfo>> nodePacketMap = aggregatedInfo.getNodePacketMap();

        buffer.putLong(aggregatedInfo.getTime());
        buffer.putInt(nodePacketMap.size());
        for(Map.Entry<Integer, List<PacketInfo>> entry : nodePacketMap.entrySet()){

            buffer.putInt(entry.getKey());
            buffer.putInt(entry.getValue().size());

            for(PacketInfo packetInfo : entry.getValue()){
                SimulationSerializers.toBinary(packetInfo, buffer);
            }
        }
    }



    public Object fromBinary(ByteBuffer buffer) {

        Map<Integer, List<PacketInfo>> result = new HashMap<Integer, List<PacketInfo>>();

        long time = buffer.getLong();
        int mapSize = buffer.getInt();
        while(mapSize > 0){

            Integer key = buffer.getInt();
            int valueSize = buffer.getInt();

            List<PacketInfo> packets = new ArrayList<PacketInfo>();
            while(valueSize > 0){

                PacketInfo packetInfo = (PacketInfo)SimulationSerializers.fromBinary(buffer);
                packets.add(packetInfo);
                valueSize --;
            }

            result.put(key, packets);
            mapSize --;
        }
        return new AggregatedInfo(time, result);
    }

    @Override
    public String toString() {
        return "AggregatedInfoSimSerializer{" +
                "identifier=" + identifier +
                '}';
    }
}
