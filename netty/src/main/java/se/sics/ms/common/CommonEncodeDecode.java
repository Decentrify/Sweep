/*
 * Copyright (C) 2009 Swedish Institute of Computer Science (SICS) Copyright (C)
 * Copyright (C) 2009 Royal Institute of Technology (KTH)
 *
 * Croupier is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */

package se.sics.ms.common;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import se.sics.gvod.common.msgs.MessageDecodingException;
import se.sics.gvod.net.BaseMsgFrameDecoder;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.msgs.RewriteableMsg;
import se.sics.ms.serializer.SearchDescriptorSerializer;
import se.sics.ms.types.SearchDescriptor;
import se.sics.p2ptoolbox.croupier.api.util.PeerView;
import se.sics.p2ptoolbox.croupier.core.CroupierNetworkSettings;
import se.sics.p2ptoolbox.gradient.core.GradientNetworkSettings;
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
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class CommonEncodeDecode extends BaseMsgFrameDecoder{

    public static final byte CROUPIER_REQUEST = (byte) 0x90;
    public static final byte CROUPIER_RESPONSE = (byte) 0x91;
    public static final byte GRADIENT_REQUEST = (byte) 0x92;
    public static final byte GRADIENT_RESPONSE = (byte) 0x93;

    //other aliases
    public static final byte HEADER_FIELD_CODE = (byte) 0x01;
    public static final byte PEER_VIEW_CODE = (byte) 0x02;

    public static final String HEADER_FIELD_ALIAS = "MY_EXAMPLE_HEADER_FIELD";
    public static final String PEER_VIEW_ALIAS = "MY_EXAMPLE_PEER_VIEW";

    private static final SerializationContext context = new SerializationContextImpl();
    
    public static void init() {
        
        NetMsg.setContext(context);
        SerializerAdapter.setContext(context);
        CroupierNetworkSettings.oneTimeSetup(context, CROUPIER_REQUEST, CROUPIER_RESPONSE);
        GradientNetworkSettings.oneTimeSetup(context, GRADIENT_REQUEST, GRADIENT_RESPONSE);
        
        try {
            context.registerAlias(HeaderField.class, HEADER_FIELD_ALIAS, HEADER_FIELD_CODE);
            context.registerSerializer(OverlayHeaderField.class, new OverlayHeaderFieldSerializer());
            context.multiplexAlias(HEADER_FIELD_ALIAS, OverlayHeaderField.class, (byte)0x01);

            context.registerSerializer(UUID.class, new UUIDSerializer());
            context.registerSerializer(VodAddress.class, new VodAddressSerializer());
            
            context.registerAlias(PeerView.class, PEER_VIEW_ALIAS, PEER_VIEW_CODE);
            context.registerSerializer(SearchDescriptor.class, new SearchDescriptorSerializer());
            context.multiplexAlias(PEER_VIEW_ALIAS, SearchDescriptor.class, (byte)0x01);
            
        } catch (SerializationContext.DuplicateException ex) {
            throw new RuntimeException(ex);
        } catch (SerializationContext.MissingException ex) {
            throw new RuntimeException(ex);
        }
    }

    public CommonEncodeDecode() {
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
            case CROUPIER_REQUEST:
                SerializerAdapter.Request cReqS = new SerializerAdapter.Request();
                return cReqS.decodeMsg(buffer);
            case CROUPIER_RESPONSE:
                SerializerAdapter.Response cRespS = new SerializerAdapter.Response();
                return cRespS.decodeMsg(buffer);
            case GRADIENT_REQUEST:
                SerializerAdapter.Request gReqS = new SerializerAdapter.Request();
                return gReqS.decodeMsg(buffer);
            case GRADIENT_RESPONSE:
                SerializerAdapter.Response gRespS = new SerializerAdapter.Response();
                return gRespS.decodeMsg(buffer);
            default:
                return null;
        }
    }
    
    
}
