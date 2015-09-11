package se.sics.ms.data.aggregator.serializer;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.ms.data.aggregator.packets.InternalStatePacket;
import se.sics.p2ptoolbox.util.network.impl.DecoratedAddress;

/**
 * Serializer for the internal state packet.
 *  
 * Created by babbarshaer on 2015-09-11.
 */
public class InternalStatePacketSerializer implements Serializer {
    
    private int id;
    
    public InternalStatePacketSerializer(int id){
        this.id = id;
    }
    
    
    @Override
    public int identifier() {
        return this.id;
    }

    @Override
    public void toBinary(Object o, ByteBuf byteBuf) {
        
        InternalStatePacket isp = (InternalStatePacket)o;
        byteBuf.writeInt(isp.getPartitionId());
        byteBuf.writeInt(isp.getPartitionDepth());
        Serializers.lookupSerializer(DecoratedAddress.class).toBinary(isp.getLeaderAddress(), byteBuf);
        byteBuf.writeLong(isp.getNumEntries());

    }

    @Override
    public Object fromBinary(ByteBuf byteBuf, Optional<Object> objectOptional) {

        int partitionId = byteBuf.readInt();
        int partitionDepth = byteBuf.readInt();
        
        DecoratedAddress leaderAddress = (DecoratedAddress) Serializers.lookupSerializer(DecoratedAddress.class).fromBinary(byteBuf, objectOptional);
        long numEntries = byteBuf.readLong();
        
        return new InternalStatePacket(partitionId, partitionDepth, leaderAddress, numEntries);
    }
}
