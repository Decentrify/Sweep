package se.sics.ms.aggregator.serializer;

import io.netty.buffer.ByteBuf;
import org.javatuples.Pair;
import se.sics.ms.aggregator.data.ComponentUpdate;
import se.sics.ms.aggregator.data.SweepAggregatedPacket;
import se.sics.p2ptoolbox.serialization.SerializationContext;
import se.sics.p2ptoolbox.serialization.Serializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Serializer for the Aggregated Packet From Sweep.
 * Created by babbar on 2015-03-17.
 */
public class SweepPacketSerializer implements Serializer<SweepAggregatedPacket> {

    @Override
    public ByteBuf encode(SerializationContext context, ByteBuf buffer, SweepAggregatedPacket obj) throws SerializerException, SerializationContext.MissingException {

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
                for(Integer id : valueMap.keySet()){
                    
                    Pair<Byte, Byte> pvCode = context.getCode(key);
                    buffer.writeByte(pvCode.getValue0());
                    buffer.writeByte(pvCode.getValue1());
                    
                    Serializer serializer = context.getSerializer(key);
                    serializer.encode(context, buffer, valueMap.get(id));
                }
            }

        }
        else{
            // Encode 0 size;
            buffer.writeInt(0);
        }
        
        return buffer;
    }


    @Override
    public SweepAggregatedPacket decode(SerializationContext serializationContext, ByteBuf buffer) throws SerializerException, SerializationContext.MissingException {
        
        int size = buffer.readInt();
        Map<Class, Map<Integer, ComponentUpdate>> componentDataMap = new HashMap<Class, Map<Integer, ComponentUpdate>>();
        SweepAggregatedPacket sap = null;
        
        while (size > 0){
            
            Byte pv0 = buffer.readByte();
            Byte pv1 = buffer.readByte();
            
            Serializer serializer = serializationContext.getSerializer(ComponentUpdate.class, pv0, pv1);
            ComponentUpdate update = (ComponentUpdate) serializer.decode(serializationContext, buffer);
            
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


    @Override
    public int getSize(SerializationContext serializationContext, SweepAggregatedPacket sweepAggregatedPacket) throws SerializerException, SerializationContext.MissingException {


        int size = 0;
        size += (Integer.SIZE/8);       // Map Length.
        
        Map<Class, Map<Integer, ComponentUpdate>> componentDataMap = sweepAggregatedPacket.getComponentDataMap();
        for(Map<Integer, ComponentUpdate> valueMap : componentDataMap.values()){
            
            for(ComponentUpdate update : valueMap.values()){

                size += 2 * (Byte.SIZE/8); // pv code
                
                Serializer serializer = serializationContext.getSerializer(update.getClass());
                size += serializer.getSize(serializationContext, update);
            }
        }
        
        
        return size;
    }
}
