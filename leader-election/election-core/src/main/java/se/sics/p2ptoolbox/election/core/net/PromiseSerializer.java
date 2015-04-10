package se.sics.p2ptoolbox.election.core.net;

import io.netty.buffer.ByteBuf;
import org.javatuples.Pair;
import se.sics.gvod.net.VodAddress;
import se.sics.p2ptoolbox.election.api.LCPeerView;
import se.sics.p2ptoolbox.election.core.data.Promise;
import se.sics.p2ptoolbox.serialization.SerializationContext;
import se.sics.p2ptoolbox.serialization.Serializer;

import java.util.UUID;

/**
 * Serializer for the promise wrapper object.
 * Created by babbar on 2015-04-02.
 */
public class PromiseSerializer {


    public static class Request implements Serializer<Promise.Request> {

        @Override
        public ByteBuf encode(SerializationContext context, ByteBuf byteBuf, Promise.Request request) throws SerializerException, SerializationContext.MissingException {

//            Pair<Byte, Byte> code = context.getCode(request.leaderView.getClass(), LCPeerView.class);
            Pair<Byte, Byte> code = context.getCode(request.leaderView.getClass(), LCPeerView.class);
            byteBuf.writeByte(code.getValue0());
            byteBuf.writeByte(code.getValue1());

            Serializer serializer = context.getSerializer(request.leaderView.getClass());
            serializer.encode(context, byteBuf, request.leaderView);

            context.getSerializer(VodAddress.class).encode(context, byteBuf, request.leaderAddress);
            context.getSerializer(UUID.class).encode(context, byteBuf, request.electionRoundId);

            return byteBuf;
        }

        @Override
        public Promise.Request decode(SerializationContext context, ByteBuf byteBuf) throws SerializerException, SerializationContext.MissingException {

            Byte pvCode0 = byteBuf.readByte();
            Byte pvCode1 = byteBuf.readByte();

            Serializer lcpSerializer = context.getSerializer(LCPeerView.class, pvCode0, pvCode1);
            LCPeerView lcp = (LCPeerView)lcpSerializer.decode(context, byteBuf);
            VodAddress address = context.getSerializer(VodAddress.class).decode(context, byteBuf);
            UUID electionRoundId = context.getSerializer(UUID.class).decode(context, byteBuf);

            return new Promise.Request(address, lcp, electionRoundId);
        }

        @Override
        public int getSize(SerializationContext context, Promise.Request request) throws SerializerException, SerializationContext.MissingException {

            int size = 0;
            size += 2* Byte.SIZE/8;

            Serializer lcvS = context.getSerializer(request.leaderView.getClass());
            size += lcvS.getSize(context, request.leaderView);
            size += context.getSerializer(VodAddress.class).getSize(context, request.leaderAddress);
            size += context.getSerializer(UUID.class).getSize(context, request.electionRoundId);
            return size;
        }
    }


    public static class Response implements Serializer<Promise.Response>{

        @Override
        public ByteBuf encode(SerializationContext serializationContext, ByteBuf byteBuf, Promise.Response response) throws SerializerException, SerializationContext.MissingException {

            byteBuf.writeBoolean(response.acceptCandidate);
            byteBuf.writeBoolean(response.isConverged);
            serializationContext.getSerializer(UUID.class).encode(serializationContext, byteBuf, response.electionRoundId);

            return byteBuf;
        }

        @Override
        public Promise.Response decode(SerializationContext serializationContext, ByteBuf byteBuf) throws SerializerException, SerializationContext.MissingException {

            boolean isAccepted = byteBuf.readBoolean();
            boolean isConverged = byteBuf.readBoolean();
            UUID electionRoundId = serializationContext.getSerializer(UUID.class).decode(serializationContext, byteBuf);

            return new Promise.Response(isAccepted, isConverged, electionRoundId);
        }

        @Override
        public int getSize(SerializationContext serializationContext, Promise.Response response) throws SerializerException, SerializationContext.MissingException {

            int size =0;
            size += 2 * Byte.SIZE/8;
            size += serializationContext.getSerializer(UUID.class).getSize(serializationContext, response.electionRoundId);

            return size;
        }
    }


}
