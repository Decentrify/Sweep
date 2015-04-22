package se.sics.ms.serializer;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import se.sics.gvod.net.util.UserTypesEncoderFactory;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.ms.data.ControlInformation;
import se.sics.ms.types.OverlayId;

import java.util.UUID;

/**
 * Serializer for the control information
 * Created by babbar on 2015-04-21.
 */
public class ControlInformationSerializer {

    public static class Request implements Serializer{

        private final int id;

        public Request(int id){
            this.id = id;
        }

        @Override
        public int identifier() {
            return this.id;
        }

        @Override
        public void toBinary(Object o, ByteBuf byteBuf) {

            ControlInformation.Request request = (ControlInformation.Request)o;
            Serializers.lookupSerializer(UUID.class).toBinary(request.getRequestId(), byteBuf);
            byteBuf.writeInt(request.getOverlayId().getId());
        }

        @Override
        public Object fromBinary(ByteBuf byteBuf, Optional<Object> optional) {

            UUID roundId  = (UUID)Serializers.lookupSerializer(UUID.class).fromBinary(byteBuf, optional);
            int overlayId = byteBuf.readInt();

            return new ControlInformation.Request(roundId, new OverlayId(overlayId));
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

            ControlInformation.Response response = (ControlInformation.Response)o;
            Serializers.lookupSerializer(UUID.class).toBinary(response.getRoundId(), byteBuf);

            byte[] bytes = response.getByteInfo();
            if(bytes == null){
                byteBuf.writeInt(0);
                bytes = new byte[0];
            }
            else{
                byteBuf.writeInt(bytes.length);
            }

            byteBuf.writeBytes(bytes);
        }

        @Override
        public Object fromBinary(ByteBuf byteBuf, Optional<Object> optional) {

            UUID roundId = (UUID)Serializers.lookupSerializer(UUID.class).fromBinary(byteBuf, optional);

            int length = byteBuf.readInt();
            byte[] bytes = new byte[length];
            byteBuf.readBytes(bytes);

            return new ControlInformation.Response(roundId, bytes);
        }
    }

}
