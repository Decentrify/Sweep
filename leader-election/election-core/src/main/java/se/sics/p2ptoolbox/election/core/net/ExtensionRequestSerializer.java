package se.sics.p2ptoolbox.election.core.net;

import io.netty.buffer.ByteBuf;
import org.javatuples.Pair;
import se.sics.gvod.net.VodAddress;
import se.sics.p2ptoolbox.election.api.LCPeerView;
import se.sics.p2ptoolbox.election.core.data.ExtensionRequest;
import se.sics.p2ptoolbox.serialization.SerializationContext;
import se.sics.p2ptoolbox.serialization.Serializer;

import java.security.PublicKey;
import java.util.UUID;

/**
 * Serializer for the Extension Request Object.
 *
 * Created by babbar on 2015-04-02.
 */
public class ExtensionRequestSerializer implements Serializer<ExtensionRequest> {

    @Override
    public ByteBuf encode(SerializationContext context, ByteBuf byteBuf, ExtensionRequest request) throws SerializerException, SerializationContext.MissingException {

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
    public ExtensionRequest decode(SerializationContext context, ByteBuf byteBuf) throws SerializerException, SerializationContext.MissingException {

        Byte value0 = byteBuf.readByte();
        Byte value1 = byteBuf.readByte();

        Serializer serializer = context.getSerializer(LCPeerView.class, value0, value1);
        LCPeerView lcpv  = (LCPeerView)serializer.decode(context, byteBuf);

        VodAddress address = context.getSerializer(VodAddress.class).decode(context, byteBuf);
        PublicKey publicKey = context.getSerializer(PublicKey.class).decode(context, byteBuf);
        UUID electionroundId = context.getSerializer(UUID.class).decode(context, byteBuf);
                
        return new ExtensionRequest(address, publicKey, lcpv, electionroundId);
    }

    @Override
    public int getSize(SerializationContext context, ExtensionRequest extensionRequest) throws SerializerException, SerializationContext.MissingException {
        int size = 0;

        size += 2* Byte.SIZE/8;
        Serializer lcvS = context.getSerializer(extensionRequest.leaderView.getClass());
        size += lcvS.getSize(context, extensionRequest.leaderView);

        size += context.getSerializer(VodAddress.class).getSize(context, extensionRequest.leaderAddress);
        size += context.getSerializer(PublicKey.class).getSize(context, extensionRequest.leaderPublicKey);
        size += context.getSerializer(UUID.class).getSize(context, extensionRequest.electionRoundId);
        
        return size;
    }
}
