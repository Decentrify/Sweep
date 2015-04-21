package se.sics.ms.serializer;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.ms.data.LeaderLookup;
import se.sics.ms.types.SearchDescriptor;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Serializer for the leader lookup protocol.
 *
 * Created by babbar on 2015-04-21.
 */
public class LeaderLookUpSerializer {


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

            LeaderLookup.Request request = (LeaderLookup.Request)o;
            Serializers.lookupSerializer(UUID.class).toBinary(request.getLeaderLookupRound(), byteBuf);
        }

        @Override
        public Object fromBinary(ByteBuf byteBuf, Optional<Object> optional) {
            UUID leaderLookupRound = (UUID) Serializers.lookupSerializer(UUID.class).fromBinary(byteBuf, optional);
            return new LeaderLookup.Request(leaderLookupRound);
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
            LeaderLookup.Response response = (LeaderLookup.Response)o;

            Serializer descriptorSerializer = Serializers.lookupSerializer(SearchDescriptor.class);
            Serializers.lookupSerializer(UUID.class).toBinary(response.getLeaderLookupRound(), byteBuf);
            byteBuf.writeBoolean(response.isLeader());

            List<SearchDescriptor> descriptorList = response.getSearchDescriptors();
            byteBuf.writeInt(descriptorList.size());

            for(SearchDescriptor desc : descriptorList){
                descriptorSerializer.toBinary(desc, byteBuf);
            }
        }

        @Override
        public Object fromBinary(ByteBuf byteBuf, Optional<Object> optional) {

            Serializer descriptorSerializer = Serializers.lookupSerializer(SearchDescriptor.class);

            UUID leaderLookUpRound = (UUID)Serializers.lookupSerializer(UUID.class).fromBinary(byteBuf, optional);
            boolean isLeader = byteBuf.readBoolean();

            int descriptorLen = byteBuf.readInt();
            List<SearchDescriptor> descriptorList = new ArrayList<SearchDescriptor>();

            while(descriptorLen > 0){
                descriptorList.add((SearchDescriptor) descriptorSerializer.fromBinary(byteBuf, optional));
                descriptorLen --;
            }

            return new LeaderLookup.Response(leaderLookUpRound, isLeader, descriptorList);
        }
    }


}
