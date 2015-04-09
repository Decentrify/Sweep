package se.sics.p2ptoolbox.election.example.util;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import se.sics.gvod.common.msgs.MessageDecodingException;
import se.sics.gvod.net.BaseMsgFrameDecoder;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.msgs.RewriteableMsg;
import se.sics.p2ptoolbox.election.api.LCPeerView;
import se.sics.p2ptoolbox.election.core.LENetworkSettings;
import se.sics.p2ptoolbox.election.example.main.LEDescriptorSerializer;
import se.sics.p2ptoolbox.election.example.main.LeaderDescriptor;
import se.sics.p2ptoolbox.serialization.SerializationContext;
import se.sics.p2ptoolbox.serialization.SerializationContextImpl;
import se.sics.p2ptoolbox.serialization.msg.HeaderField;
import se.sics.p2ptoolbox.serialization.msg.NetMsg;
import se.sics.p2ptoolbox.serialization.msg.OverlayHeaderField;
import se.sics.p2ptoolbox.serialization.serializer.OverlayHeaderFieldSerializer;
import se.sics.p2ptoolbox.serialization.serializer.SerializerAdapter;
import se.sics.p2ptoolbox.serialization.serializer.UUIDSerializer;
import se.sics.p2ptoolbox.serialization.serializer.VodAddressSerializer;

import java.util.UUID;

/**
 * Main Encode Decode Class for the Leader Election Protocol.
 *
 * Created by babbar on 2015-04-01.
 */
public class LeaderEncodeDecode extends BaseMsgFrameDecoder{


    public static final byte HEADER_FIELD_CODE = (byte) 0x01;
    public static final byte LEADER_VIEW_CODE = (byte) 0x02;


    public static final byte PROMISE_REQUEST = (byte)0x01;
    public static final byte PROMISE_RESPONSE = (byte)0x02;
    public static final byte EXTENSION_REQUEST= (byte)0x03;
    public static final byte LEASE_REQUEST = (byte)0x04;
    public static final byte LEASE_RESPONSE = (byte)0x05;

//    public static final String HEADER_FIELD_ALIAS = "SWEEP_HEADER_FIELD";
    public static final String LEADER_VIEW = "LEADER_VIEW";


    private static final SerializationContext context = new SerializationContextImpl();

    public static void init(){

        NetMsg.setContext(context);
        SerializerAdapter.setContext(context);


        try {
//            context.registerAlias(HeaderField.class, HEADER_FIELD_ALIAS, HEADER_FIELD_CODE);
//            context.registerSerializer(OverlayHeaderField.class, new OverlayHeaderFieldSerializer());
//            context.multiplexAlias(HEADER_FIELD_ALIAS, OverlayHeaderField.class, (byte)0x01);

            context.registerSerializer(UUID.class, new UUIDSerializer());
            context.registerSerializer(VodAddress.class, new VodAddressSerializer());

            context.registerAlias(LCPeerView.class, LEADER_VIEW, LEADER_VIEW_CODE);
            context.registerSerializer(LeaderDescriptor.class, new LEDescriptorSerializer());
            context.multiplexAlias(LEADER_VIEW, LeaderDescriptor.class, (byte)0x01);


        } catch (SerializationContext.DuplicateException ex) {
            throw new RuntimeException(ex);
        } catch (SerializationContext.MissingException ex) {
            throw new RuntimeException(ex);
        }

        // One time settings.
        LENetworkSettings.oneTimeSetup(context, PROMISE_REQUEST, PROMISE_RESPONSE, EXTENSION_REQUEST, LEASE_REQUEST, LEASE_RESPONSE);
    }


    public LeaderEncodeDecode() {
        super();
    }

    @Override
    protected RewriteableMsg decodeMsg(ChannelHandlerContext ctx, ByteBuf buffer) throws MessageDecodingException {
        // See if msg is part of parent project, if yes then return it.
        // Otherwise decode the msg here.
        RewriteableMsg msg = super.decodeMsg(ctx, buffer);
        if (msg != null) {
            return msg;
        }

        switch (opKod) {
            case PROMISE_REQUEST:
                SerializerAdapter.Request cReqS = new SerializerAdapter.Request();
                return cReqS.decodeMsg(buffer);
            case PROMISE_RESPONSE:
                SerializerAdapter.Response cRespS = new SerializerAdapter.Response();
                return cRespS.decodeMsg(buffer);
            case EXTENSION_REQUEST:
                SerializerAdapter.OneWay gReqS = new SerializerAdapter.OneWay();
                return gReqS.decodeMsg(buffer);
            case LEASE_REQUEST:
                SerializerAdapter.Request lReqS = new SerializerAdapter.Request();
                return lReqS.decodeMsg(buffer);
            case LEASE_RESPONSE:
                SerializerAdapter.Response lRespS = new SerializerAdapter.Response();
                return lRespS.decodeMsg(buffer);
            default:
                return null;
        }
    }





}
