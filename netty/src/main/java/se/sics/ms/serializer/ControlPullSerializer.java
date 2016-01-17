package se.sics.ms.serializer;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.ms.data.ControlPull;
import se.sics.ms.helper.SerializerDecoderHelper;
import se.sics.ms.helper.SerializerEncoderHelper;
import se.sics.ms.types.LeaderUnit;
import se.sics.ms.types.OverlayId;

import java.security.PublicKey;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import se.sics.ktoolbox.util.network.KAddress;

/**
 * Serializer for the information exchange as part of
 * Control Pull Mechanism.
 *
 * Created by babbarshaer on 2015-06-22.
 */
public class ControlPullSerializer {
    
    
    
    public static class Request implements Serializer {

        private int id;
        
        public Request(int id){
            this.id = id;
        }

        @Override
        public int identifier() {
            return this.id;
        }

        @Override
        public void toBinary(Object o, ByteBuf buf) {
            
            ControlPull.Request request = (ControlPull.Request)o;
            
            LeaderUnit lastUnit = request.getLeaderUnit();
            SerializerEncoderHelper.checkNullAndUpdateBuff(buf, lastUnit);
            
            if(lastUnit != null){
               Serializers.toBinary(lastUnit, buf);
            }
            
            Serializers.lookupSerializer(UUID.class).toBinary(request.getPullRound(), buf);
            Serializers.lookupSerializer(OverlayId.class).toBinary(request.getOverlayId(), buf);
            
        }

        @Override
        public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
            
            boolean isNull = SerializerDecoderHelper.checkNullCommit(buf);
            LeaderUnit unit = null;
            
            if(!isNull){
                unit  = (LeaderUnit) Serializers.fromBinary(buf, hint);
            }
            
            UUID pullRound = (UUID)Serializers.lookupSerializer(UUID.class).fromBinary(buf, hint);
            OverlayId overlayId = (OverlayId)Serializers.lookupSerializer(OverlayId.class).fromBinary(buf, hint);
            
            return new ControlPull.Request(pullRound, overlayId, unit);
        }
    }
    
    
    
    public static class Response implements Serializer{

        private int id;

        public Response(int id){
            this.id = id;
        }
        
        @Override
        public int identifier() {
            return this.id;
        }

        @Override
        public void toBinary(Object o, ByteBuf buf) {
            
            ControlPull.Response response = (ControlPull.Response)o;

            Serializers.lookupSerializer(UUID.class).toBinary(response.getPullRound(), buf);
            
            KAddress leaderAddress = response.getLeaderAddress();
            SerializerEncoderHelper.checkNullAndUpdateBuff(buf, response.getLeaderAddress());
            
            if(leaderAddress != null){
                Serializers.toBinary(leaderAddress, buf);
            }
            
            PublicKey leaderKey = response.getLeaderKey();
            SerializerEncoderHelper.checkNullAndUpdateBuff(buf, leaderKey);
            
            if(leaderKey != null){
                Serializers.lookupSerializer(PublicKey.class)
                        .toBinary(response.getLeaderKey(), buf);    
            }
            
            List<LeaderUnit> units = response.getNextUpdates();
            int size = units.size();
            buf.writeInt(size);
            
            for(LeaderUnit unit : units){
                Serializers.toBinary(unit, buf);
            }
            
            buf.writeInt(response.getOverlayId());
        }

        @Override
        public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
            
            
            UUID pullRound = (UUID)Serializers.lookupSerializer(UUID.class).fromBinary(buf, hint);
            
            boolean addressAbsent = SerializerDecoderHelper.checkNullCommit(buf);
            KAddress leaderAddress = null;
            if( !addressAbsent ){
                
                leaderAddress = (KAddress)Serializers.fromBinary(buf, hint);
            }

            boolean keyAbsent = SerializerDecoderHelper.checkNullCommit(buf);
            PublicKey leaderKey = null;
            
            if(!keyAbsent){
                
                leaderKey = (PublicKey)Serializers.lookupSerializer(PublicKey.class)
                        .fromBinary(buf, hint);
            }

            int size = buf.readInt();
            List<LeaderUnit> nextUnits = new ArrayList<LeaderUnit>();
            
            while(size > 0){
                
                LeaderUnit unit = (LeaderUnit)Serializers.fromBinary(buf, hint);
                nextUnits.add(unit);
                
                size --;
            }
            
            int overlayId = buf.readInt();
            return new ControlPull.Response(pullRound, leaderAddress, leaderKey, nextUnits, overlayId);
        }
    }
    
    
    
}
