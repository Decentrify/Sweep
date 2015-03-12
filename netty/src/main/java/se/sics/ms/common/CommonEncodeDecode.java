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
import se.sics.ms.net.MessageFrameDecoder;
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
public class CommonEncodeDecode {

    //other aliases
    public static final byte HEADER_FIELD_CODE = (byte) 0x01;
    public static final byte PEER_VIEW_CODE = (byte) 0x02;

    public static final String HEADER_FIELD_ALIAS = "SWEEP_HEADER_FIELD";
    public static final String PEER_VIEW_ALIAS = "SWEEP_PEER_VIEW";

    private static final SerializationContext context = new SerializationContextImpl();
    
    public static void init() {
        
        NetMsg.setContext(context);
        SerializerAdapter.setContext(context);
        
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
        CroupierNetworkSettings.oneTimeSetup(context, MessageFrameDecoder.CROUPIER_REQUEST, MessageFrameDecoder.CROUPIER_RESPONSE);
        GradientNetworkSettings.oneTimeSetup(context, MessageFrameDecoder.GRADIENT_REQUEST, MessageFrameDecoder.GRADIENT_RESPONSE);
    }
    
}
