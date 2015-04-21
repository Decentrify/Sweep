package se.sics.ms.serializer;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.ms.data.Repair;
import se.sics.ms.types.IndexEntry;

import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;

/**
 * Serializer for the Repair Container during the repair protocol.
 *
 * Created by babbar on 2015-04-21.
 */
public class RepairSerializer {



    public static class Request implements Serializer{

        private final int id;

        public Request(int id) {
            this.id = id;
        }

        @Override
        public int identifier() {
            return this.id;
        }

        @Override
        public void toBinary(Object o, ByteBuf byteBuf) {

            Repair.Request request = (Repair.Request)o;
            Serializers.lookupSerializer(UUID.class).toBinary(request.getRepairRoundId(), byteBuf);
            Long[] missingIds = request.getMissingIds();

            if(missingIds == null){

                byteBuf.writeInt(0);
                missingIds = new Long[0];
            }
            else{
                byteBuf.writeInt(missingIds.length);
            }

            for(long val : missingIds){
                byteBuf.writeLong(val);
            }

        }

        @Override
        public Object fromBinary(ByteBuf byteBuf, Optional<Object> optional) {

            UUID repairRound = (UUID)Serializers.lookupSerializer(UUID.class).fromBinary(byteBuf, optional);
            int missingIdLength = byteBuf.readInt();
            Long[] missingIds = new Long [missingIdLength];

            for(int i=0 ; i <missingIdLength; i++){
                missingIds[i] = byteBuf.readLong();
            }

            return new Repair.Request(repairRound, missingIds);
        }
    }



    public static class Response implements Serializer{

        private final int id;

        public Response(int id) {
            this.id = id;
        }

        @Override
        public int identifier() {
            return this.id;
        }

        @Override
        public void toBinary(Object o, ByteBuf byteBuf) {

            Repair.Response response = (Repair.Response)o;
            Serializers.lookupSerializer(UUID.class).toBinary(response.getRepairRoundId(), byteBuf);
            Serializer entrySerializer = Serializers.lookupSerializer(IndexEntry.class);

            Collection<IndexEntry> entryCollection = response.getMissingEntries();

            if(entryCollection == null){
                byteBuf.writeInt(0);
                entryCollection =new ArrayList<IndexEntry>();
            }
            else{
                byteBuf.writeInt(entryCollection.size());
            }

            for(IndexEntry entry: entryCollection){
                entrySerializer.toBinary(entry, byteBuf);
            }

        }

        @Override
        public Object fromBinary(ByteBuf byteBuf, Optional<Object> optional) {


            UUID repairRound = (UUID) Serializers.lookupSerializer(UUID.class).fromBinary(byteBuf, optional);
            Serializer entrySerializer = Serializers.lookupSerializer(IndexEntry.class);

            int len = byteBuf.readInt();

            Collection<IndexEntry> entryCollection  = new ArrayList<IndexEntry>();
            while(len >0){

                IndexEntry entry = (IndexEntry) entrySerializer.fromBinary(byteBuf, optional);
                entryCollection.add(entry);
                len--;
            }

            return new Repair.Response(repairRound, entryCollection);
        }
    }

}
