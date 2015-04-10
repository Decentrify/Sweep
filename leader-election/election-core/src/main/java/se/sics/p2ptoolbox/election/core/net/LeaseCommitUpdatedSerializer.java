package se.sics.p2ptoolbox.election.core.net;

import io.netty.buffer.ByteBuf;
import org.javatuples.Pair;
import se.sics.gvod.net.VodAddress;
import se.sics.p2ptoolbox.election.api.LCPeerView;
import se.sics.p2ptoolbox.election.core.data.LeaseCommit;
import se.sics.p2ptoolbox.election.core.data.LeaseCommitUpdated;
import se.sics.p2ptoolbox.serialization.SerializationContext;
import se.sics.p2ptoolbox.serialization.Serializer;

import java.security.PublicKey;
import java.util.UUID;

/**
 * Serializer wrapper for the Lease Commit Object.
 *
 * Created by babbar on 2015-04-09.
 */
public class LeaseCommitUpdatedSerializer {


    public static class Request implements Serializer<LeaseCommitUpdated.Request> {

        @Override
        public ByteBuf encode(SerializationContext context, ByteBuf byteBuf, LeaseCommitUpdated.Request request) throws SerializerException, SerializationContext.MissingException {

            Pair<Byte, Byte> code = context.getCode(request.leaderView.getClass(), LCPeerView.class);
            byteBuf.writeByte(code.getValue0());
            byteBuf.writeByte(code.getValue1());

            Serializer serializer = context.getSerializer(request.leaderView.getClass());
            serializer.encode(context, byteBuf, request.leaderView);

            context.getSerializer(VodAddress.class).encode(context, byteBuf, request.leaderAddress);
            context.getSerializer(PublicKey.class).encode(context, byteBuf, request.leaderPublicKey);
            context.getSerializer(UUID.class).encode(context, byteBuf, request.electionRoundId);

            return byteBuf;
        }

        @Override
        public LeaseCommitUpdated.Request decode(SerializationContext context, ByteBuf byteBuf) throws SerializerException, SerializationContext.MissingException {

            Byte value0 = byteBuf.readByte();
            Byte value1 = byteBuf.readByte();

            Serializer serializer = context.getSerializer(LCPeerView.class, value0, value1);
            LCPeerView lcpv  = (LCPeerView)serializer.decode(context, byteBuf);

            VodAddress address = context.getSerializer(VodAddress.class).decode(context, byteBuf);
            PublicKey publicKey = context.getSerializer(PublicKey.class).decode(context, byteBuf);
            UUID electionRoundId = context.getSerializer(UUID.class).decode(context, byteBuf);

            return new LeaseCommitUpdated.Request(address, publicKey, lcpv, electionRoundId);
        }

        @Override
        public int getSize(SerializationContext context, LeaseCommitUpdated.Request request) throws SerializerException, SerializationContext.MissingException {

            int size = 0;

            size += 2* Byte.SIZE/8;
            Serializer lcvS = context.getSerializer(request.leaderView.getClass());
            size += lcvS.getSize(context, request.leaderView);

            size += context.getSerializer(VodAddress.class).getSize(context, request.leaderAddress);
            size += context.getSerializer(PublicKey.class).getSize(context, request.leaderPublicKey);
            size += context.getSerializer(UUID.class).getSize(context, request.electionRoundId);

            return size;
        }
    }


    public static class Response implements Serializer<LeaseCommitUpdated.Response>{

        @Override
        public ByteBuf encode(SerializationContext context, ByteBuf byteBuf, LeaseCommitUpdated.Response response) throws SerializerException, SerializationContext.MissingException {

            byteBuf.writeBoolean(response.isCommit);
            context.getSerializer(UUID.class).encode(context, byteBuf, response.electionRoundId);

            return byteBuf;
        }

        @Override
        public LeaseCommitUpdated.Response decode(SerializationContext context, ByteBuf byteBuf) throws SerializerException, SerializationContext.MissingException {

            boolean isCommit = byteBuf.readBoolean();
            UUID electionRoundId = context.getSerializer(UUID.class).decode(context, byteBuf);

            return new LeaseCommitUpdated.Response(isCommit, electionRoundId);
        }

        @Override
        public int getSize(SerializationContext context, LeaseCommitUpdated.Response response) throws SerializerException, SerializationContext.MissingException {

            int size =0;

            size += Byte.SIZE/8;
            size += context.getSerializer(UUID.class).getSize(context, response.electionRoundId);
            return size;
        }
    }
}
