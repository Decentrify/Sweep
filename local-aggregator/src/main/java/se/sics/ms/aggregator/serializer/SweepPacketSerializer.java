package se.sics.ms.aggregator.serializer;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.ms.aggregator.data.ComponentUpdate;
import se.sics.ms.aggregator.data.SweepAggregatedPacket;


import java.util.HashMap;
import java.util.Map;

/**
 * Serializer for the Aggregated Packet From Sweep.
 * Created by babbar on 2015-03-17.
 */
public class SweepPacketSerializer implements Serializer {

    private final int id;

    public SweepPacketSerializer(int id){
        this.id = id;
    }

    @Override
    public int identifier() {
        return this.id;
    }

    @Override
    public void toBinary(Object o, ByteBuf buffer) {

        SweepAggregatedPacket obj = (SweepAggregatedPacket)o;

        Map<Class, Map<Integer, ComponentUpdate>> dataMap = obj.getComponentDataMap();

        if(dataMap != null  && dataMap.size() > 0 ){

            int size = 0;

            for(Class key : dataMap.keySet()){
                size += dataMap.get(key).size();
            }

            // Write map size;
            buffer.writeInt(size);


            // Now perform the actual serialization.
            for(Class key : dataMap.keySet()){

                Map<Integer, ComponentUpdate> valueMap = dataMap.get(key);

                for(Map.Entry<Integer, ComponentUpdate> entry : valueMap.entrySet()){
                    Serializers.toBinary(entry.getValue(), buffer);
                }
            }

        }
        else{
            // Encode 0 size;
            buffer.writeInt(0);
        }

    }

    @Override
    public Object fromBinary(ByteBuf buffer, Optional<Object> optional) {

        int size = buffer.readInt();
        Map<Class, Map<Integer, ComponentUpdate>> componentDataMap = new HashMap<Class, Map<Integer, ComponentUpdate>>();
        SweepAggregatedPacket sap = null;

        while (size > 0){

            ComponentUpdate update = (ComponentUpdate)Serializers.fromBinary(buffer, optional);

            Map<Integer, ComponentUpdate> value;
            if(componentDataMap.get(update.getClass())== null){
                value = new HashMap<Integer, ComponentUpdate>();
            }
            else{
                value = componentDataMap.get(update.getClass());
            }

            value.put(update.getComponentOverlay(), update);
            componentDataMap.put(update.getClass(), value);

            size --;
        }

        sap  = new SweepAggregatedPacket(componentDataMap);
        return sap;
    }
}
