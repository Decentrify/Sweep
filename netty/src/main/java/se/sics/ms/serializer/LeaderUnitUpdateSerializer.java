package se.sics.ms.serializer;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.ms.helper.SerializerDecoderHelper;
import se.sics.ms.helper.SerializerEncoderHelper;
import se.sics.ms.types.LeaderUnit;
import se.sics.ms.types.LeaderUnitUpdate;

/**
 * Serializer for the unit update packet sent as part of the
 * prepare phase of the sharding protocol.
 *
 * Created by babbar on 2015-06-22.
 */
public class LeaderUnitUpdateSerializer implements Serializer{

    private int id;

    public LeaderUnitUpdateSerializer(int id){
        this.id = id;
    }

    @Override
    public int identifier() {
        return this.id;
    }

    @Override
    public void toBinary(Object o, ByteBuf buf) {

        LeaderUnitUpdate unitUpdate = (LeaderUnitUpdate)o;
        LeaderUnit currentUpdate = unitUpdate.getCurrentEpochUpdate();
        Serializers.toBinary(currentUpdate, buf);

        LeaderUnit previousUpdate = unitUpdate.getPreviousEpochUpdate();
        SerializerEncoderHelper.checkNullAndUpdateBuff(buf, previousUpdate);

        if(previousUpdate != null){
            Serializers.toBinary(previousUpdate, buf);
        }
    }

    @Override
    public Object fromBinary(ByteBuf buf, Optional<Object> hint) {

        LeaderUnit currentUpdate = (LeaderUnit)Serializers.fromBinary(buf, hint);
        boolean isNull = SerializerDecoderHelper.checkNullCommit(buf);

        LeaderUnit previousUpdate = null;
        if(!isNull){
            previousUpdate = (LeaderUnit)Serializers.fromBinary(buf, hint);
        }

        return new LeaderUnitUpdate(previousUpdate, currentUpdate);
    }
}
