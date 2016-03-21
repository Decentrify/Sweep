//package se.sics.ms.serializer;
//
//import com.google.common.base.Optional;
//import io.netty.buffer.ByteBuf;
//import se.sics.kompics.network.netty.serialization.Serializer;
//import se.sics.kompics.network.netty.serialization.Serializers;
//import se.sics.ms.data.InternalStatePacket;
//import se.sics.ms.helper.SerializerDecoderHelper;
//import se.sics.ms.helper.SerializerEncoderHelper;
//import se.sics.p2ptoolbox.util.network.impl.DecoratedAddress;
//
///**
// * Serializer for the internal state packet.
// *  
// * Created by babbarshaer on 2015-09-11.
// */
//public class InternalStatePacketSerializer implements Serializer {
//    
//    private int id;
//    
//    public InternalStatePacketSerializer(int id){
//        this.id = id;
//    }
//    
//    
//    @Override
//    public int identifier() {
//        return this.id;
//    }
//
//    @Override
//    public void toBinary(Object o, ByteBuf byteBuf) {
//        
//        InternalStatePacket isp = (InternalStatePacket)o;
//        byteBuf.writeInt(isp.getSelfIdentifier());
//        byteBuf.writeInt(isp.getPartitionId());
//        byteBuf.writeInt(isp.getPartitionDepth());
//
//        Integer leader = isp.getLeaderIdentifier();
//        if(leader == null){
//            byteBuf.writeByte(0);
//        }
//        else{
//            byteBuf.writeByte(1);
//            byteBuf.writeInt(leader);
//        }
//
//        byteBuf.writeLong(isp.getNumEntries());
//    }
//
//    @Override
//    public Object fromBinary(ByteBuf byteBuf, Optional<Object> objectOptional) {
//
//        Integer selfIndentifier = byteBuf.readInt();
//        int partitionId = byteBuf.readInt();
//        int partitionDepth = byteBuf.readInt();
//
//        int isLeader = Byte.valueOf(byteBuf.readByte()).intValue();
//        Integer leaderId = null;
//        if(isLeader !=0 ) {
//            leaderId = byteBuf.readInt();
//        }
//
//        long numEntries = byteBuf.readLong();
//        return new InternalStatePacket(selfIndentifier, partitionId, partitionDepth, leaderId, numEntries);
//    }
//}
