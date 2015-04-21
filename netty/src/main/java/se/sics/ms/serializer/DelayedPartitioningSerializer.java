package se.sics.ms.serializer;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.ms.data.DelayedPartitioning;
import se.sics.ms.util.PartitionHelper;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

/**
 * Serializer for the DelayedPartitioning Message.
 *
 * Created by babbar on 2015-04-21.
 */
public class DelayedPartitioningSerializer {

    public static class Request implements Serializer{

        private final int id;

        public Request (int id){
            this.id = id;
        }

        @Override
        public int identifier() {
            return this.id;
        }

        @Override
        public void toBinary(Object o, ByteBuf byteBuf) {

            Serializer uuidSerializer = Serializers.lookupSerializer(UUID.class);

            DelayedPartitioning.Request request = (DelayedPartitioning.Request)o;
            uuidSerializer.toBinary(request.getRoundId(), byteBuf);

            List<UUID> partitionIdsList = request.getPartitionRequestIds();
            byteBuf.writeInt(partitionIdsList.size());

            for(UUID partitionId : partitionIdsList){
                uuidSerializer.toBinary(partitionId, byteBuf);
            }

        }

        @Override
        public Object fromBinary(ByteBuf byteBuf, Optional<Object> optional) {

            Serializer uuidSerializer = Serializers.lookupSerializer(UUID.class);

            UUID roundId = (UUID) uuidSerializer.fromBinary(byteBuf, optional);
            int len = byteBuf.readInt();
            List<UUID> partitionIds = new ArrayList<UUID>();

            while(len > 0){

                UUID uuid = (UUID)uuidSerializer.fromBinary(byteBuf, optional);
                partitionIds.add(uuid);
                len --;
            }

            return new DelayedPartitioning.Request(roundId, partitionIds);
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

            DelayedPartitioning.Response response = (DelayedPartitioning.Response)o;

            Serializer partitionInfoSerializer = Serializers.lookupSerializer(PartitionHelper.PartitionInfo.class);
            Serializer uuidSerializer = Serializers.lookupSerializer(UUID.class);

            uuidSerializer.toBinary(response.getRoundId(), byteBuf);
            List<PartitionHelper.PartitionInfo> partitionInfoList = response.getPartitionHistory();

            byteBuf.writeInt(partitionInfoList.size());
            for(PartitionHelper.PartitionInfo info : partitionInfoList){
                partitionInfoSerializer.toBinary(info, byteBuf);
            }
        }

        @Override
        public Object fromBinary(ByteBuf byteBuf, Optional<Object> optional) {

            Serializer partitionInfoSerializer = Serializers.lookupSerializer(PartitionHelper.PartitionInfo.class);
            Serializer uuidSerializer = Serializers.lookupSerializer(UUID.class);


            UUID roundId = (UUID)uuidSerializer.fromBinary(byteBuf, optional);
            int partitionHistoryLen = byteBuf.readInt();
            LinkedList<PartitionHelper.PartitionInfo> partitionHistory = new LinkedList<PartitionHelper.PartitionInfo>();

            while(partitionHistoryLen > 0) {

                PartitionHelper.PartitionInfo partitionInfo = (PartitionHelper.PartitionInfo) partitionInfoSerializer.fromBinary(byteBuf, optional);
                partitionHistory.add(partitionInfo);
                partitionHistoryLen --;
            }

            return new DelayedPartitioning.Response(roundId, partitionHistory);
        }
    }


}
