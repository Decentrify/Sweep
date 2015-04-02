package se.sics.p2ptoolbox.election.core.net;

import io.netty.buffer.ByteBuf;
import org.javatuples.Pair;
import se.sics.gvod.net.VodAddress;
import se.sics.p2ptoolbox.election.api.LCPeerView;
import se.sics.p2ptoolbox.election.core.data.LeaseCommit;
import se.sics.p2ptoolbox.serialization.SerializationContext;
import se.sics.p2ptoolbox.serialization.Serializer;

import java.security.PublicKey;

/**
 * Serializer for the Lease Commit Message.
 * Created by babbar on 2015-04-02.
 */
public class LeaseCommitSerializer implements Serializer<LeaseCommit>{

    @Override
    public ByteBuf encode(SerializationContext context, ByteBuf byteBuf, LeaseCommit request) throws SerializerException, SerializationContext.MissingException {

        Pair<Byte, Byte> code = context.getCode(request.leaderView.getClass());
        byteBuf.writeByte(code.getValue0());
        byteBuf.writeByte(code.getValue1());

        Serializer serializer = context.getSerializer(request.leaderView.getClass());
        serializer.encode(context, byteBuf, request.leaderAddress);

        context.getSerializer(VodAddress.class).encode(context, byteBuf, request.leaderAddress);
        context.getSerializer(PublicKey.class).encode(context, byteBuf, request.leaderPublicKey);

        return byteBuf;

    }

    @Override
    public LeaseCommit decode(SerializationContext context, ByteBuf byteBuf) throws SerializerException, SerializationContext.MissingException {
        Byte value0 = byteBuf.readByte();
        Byte value1 = byteBuf.readByte();

        Serializer serializer = context.getSerializer(LCPeerView.class, value0, value1);
        LCPeerView lcpv  = (LCPeerView)serializer.decode(context, byteBuf);

        VodAddress address = context.getSerializer(VodAddress.class).decode(context, byteBuf);
        PublicKey publicKey = context.getSerializer(PublicKey.class).decode(context, byteBuf);

        return new LeaseCommit(address, publicKey, lcpv);
    }

    @Override
    public int getSize(SerializationContext context, LeaseCommit leaseCommit) throws SerializerException, SerializationContext.MissingException {
        int size = 0;

        size += 2* Byte.SIZE/8;
        Serializer lcvS = context.getSerializer(leaseCommit.leaderView.getClass());
        size += lcvS.getSize(context, leaseCommit.leaderView);

        size += context.getSerializer(VodAddress.class).getSize(context, leaseCommit.leaderAddress);
        size += context.getSerializer(PublicKey.class).getSize(context, leaseCommit.leaderPublicKey);
        return size;
    }
}
