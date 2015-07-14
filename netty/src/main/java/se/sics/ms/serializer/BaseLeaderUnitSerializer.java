package se.sics.ms.serializer;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.ms.types.BaseLeaderUnit;
import se.sics.ms.types.LeaderUnit;

/**
 * Serializer for the Basic Leader Unit
 * used in the system.
 *
 * Created by babbar on 2015-06-24.
 */
public class BaseLeaderUnitSerializer implements Serializer{

    private int id;

    public BaseLeaderUnitSerializer(int id){
        this.id = id;
    }

    @Override
    public int identifier() {
        return this.id;
    }

    @Override
    public void toBinary(Object o, ByteBuf buf) {

        BaseLeaderUnit unit = (BaseLeaderUnit)o;
        buf.writeLong(unit.getEpochId());
        buf.writeInt(unit.getLeaderId());
        buf.writeLong(unit.getNumEntries());
        buf.writeInt(unit.getLeaderUnitStatus().ordinal());

    }

    @Override
    public Object fromBinary(ByteBuf buf, Optional<Object> hint) {


        long epochId = buf.readLong();
        int leaderId = buf.readInt();
        long numEntries = buf.readLong();

        LeaderUnit.LUStatus status = LeaderUnit.LUStatus.values()[buf.readInt()];

        return new BaseLeaderUnit(epochId, leaderId, numEntries, status);
    }
}
