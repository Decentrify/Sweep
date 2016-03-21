//
//package se.sics.ms.serializer;
//
//import com.google.common.base.Optional;
//import io.netty.buffer.ByteBuf;
//import se.sics.kompics.network.netty.serialization.Serializer;
//import se.sics.kompics.network.netty.serialization.Serializers;
//import se.sics.ktoolbox.aggregator.server.event.AggregatedInfo;
//import se.sics.ktoolbox.aggregator.util.PacketInfo;
//
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
///**
// * Serializer  for the aggregated information triggered
// * by the global aggregator.
// *
// * Created by babbar on 2015-09-18.
// */
//public class AggregatedInfoSerializer implements Serializer {
//
//
//    private int id;
//
//    public AggregatedInfoSerializer(int id){
//        this.id = id;
//    }
//
//    @Override
//    public int identifier() {
//        return this.id;
//    }
//
//    @Override
//    public void toBinary(Object o, ByteBuf byteBuf) {
//
//        AggregatedInfo aggregatedInfo = (AggregatedInfo)o;
//        Map<Integer, List<PacketInfo>> nodePacketMap = aggregatedInfo.getNodePacketMap();
//
////     Write the timestamp.
//        byteBuf.writeLong(aggregatedInfo.getTime());
//
////      Write the size of the map.
//        byteBuf.writeInt(nodePacketMap.size());
//
////      Then write the entries in the map.
//        for(Map.Entry<Integer, List<PacketInfo>> entry : nodePacketMap.entrySet()){
//
//            byteBuf.writeInt(entry.getKey());
//            byteBuf.writeInt(entry.getValue().size());
//
//            for(PacketInfo info : entry.getValue()){
//                Serializers.toBinary(info, byteBuf);
//            }
//        }
//
//    }
//
//    @Override
//    public Object fromBinary(ByteBuf byteBuf, Optional<Object> optional) {
//
//        Map<Integer, List<PacketInfo>> nodePacketMap = new HashMap<Integer, List<PacketInfo>>();
//
////      Time for the window.
//        long time = byteBuf.readLong();
//
////      Read Size of Packet Map.
//        int packetMapSize = byteBuf.readInt();
//        while(packetMapSize > 0){
//
////
//            Integer key = byteBuf.readInt();
//            List<PacketInfo> packets = new ArrayList<PacketInfo>();
//
//            int packetSize =byteBuf.readInt();
//            while(packetSize > 0){
//
//                PacketInfo packetInfo = (PacketInfo) Serializers.fromBinary(byteBuf, optional);
//                packets.add(packetInfo);
//                packetSize --;
//            }
//
//            nodePacketMap.put(key, packets);
//            packetMapSize --;
//        }
//
//        return new AggregatedInfo(time, nodePacketMap);
//    }
//}
