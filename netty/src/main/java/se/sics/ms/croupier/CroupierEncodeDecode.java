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

package se.sics.ms.croupier;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import java.util.UUID;
import se.sics.gvod.common.msgs.MessageDecodingException;
import se.sics.gvod.net.BaseMsgFrameDecoder;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.msgs.RewriteableMsg;
import se.sics.ms.net.MessageFrameDecoder;
import se.sics.ms.types.SearchDescriptor;
import se.sics.ms.serializer.SearchDescriptorSerializer;
import se.sics.p2ptoolbox.croupier.core.CroupierConfig;
import se.sics.p2ptoolbox.croupier.core.CroupierSetup;
import se.sics.p2ptoolbox.serialization.SerializationContext;
import se.sics.p2ptoolbox.serialization.SerializationContextImpl;
import se.sics.p2ptoolbox.serialization.msg.NetMsg;
import se.sics.p2ptoolbox.serialization.msg.OverlayHeaderField;
import se.sics.p2ptoolbox.serialization.serializer.OverlayHeaderFieldSerializer;
import se.sics.p2ptoolbox.serialization.serializer.SerializerAdapter;
import se.sics.p2ptoolbox.serialization.serializer.UUIDSerializer;
import se.sics.p2ptoolbox.serialization.serializer.VodAddressSerializer;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class CroupierEncodeDecode {

    //other aliases
    public static final byte HEADER_FIELD = (byte) 0x01;
    public static final byte PEER_VIEW = (byte) 0x02;

    private static SerializationContext context = new SerializationContextImpl();

    public static void reset() {
        context = new SerializationContextImpl();
    }
    
    public static void init() {
        try {
            context.registerAlias(CroupierConfig.MsgAliases.CROUPIER_NET_REQUEST.aliasedClass, CroupierConfig.MsgAliases.CROUPIER_NET_REQUEST.toString(), MessageFrameDecoder.CROUPIER_REQUEST);
            context.registerAlias(CroupierConfig.MsgAliases.CROUPIER_NET_RESPONSE.aliasedClass, CroupierConfig.MsgAliases.CROUPIER_NET_RESPONSE.toString(), MessageFrameDecoder.CROUPIER_RESPONSE);
            
            context.registerAlias(CroupierConfig.OtherAliases.HEADER_FIELD.aliasedClass, CroupierConfig.OtherAliases.HEADER_FIELD.toString(), HEADER_FIELD);
            context.registerSerializer(OverlayHeaderField.class, new OverlayHeaderFieldSerializer());
            context.multiplexAlias(CroupierConfig.OtherAliases.HEADER_FIELD.toString(), OverlayHeaderField.class, (byte)0x01);
            
            context.registerAlias(CroupierConfig.OtherAliases.PEER_VIEW.aliasedClass, CroupierConfig.OtherAliases.PEER_VIEW.toString(), PEER_VIEW);
            //TODO Alex/Croupier register your PeerView and PeerView Serializer here - you can have more than one, just give them different ids, but each croupier overlay can only exchange one type of PeerView
            // Registering the serializer with croupier.
            context.registerSerializer(SearchDescriptor.class, new SearchDescriptorSerializer());
            context.multiplexAlias(CroupierConfig.OtherAliases.PEER_VIEW.toString(), SearchDescriptor.class, (byte)0x01);
            
            context.registerSerializer(UUID.class, new UUIDSerializer());
            context.registerSerializer(VodAddress.class, new VodAddressSerializer());
        } catch (SerializationContext.DuplicateException ex) {
            throw new RuntimeException(ex);
        } catch (SerializationContext.MissingException ex) {
            throw new RuntimeException(ex);
        }

        NetMsg.setContext(context);
        SerializerAdapter.setContext(context);
        CroupierSetup.oneTimeSetup(context);
    }

    public CroupierEncodeDecode() {
        super();
    }

}
