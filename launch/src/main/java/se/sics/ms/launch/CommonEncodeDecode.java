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

package se.sics.ms.launch;

import se.sics.gvod.net.VodAddress;
import se.sics.ms.aggregator.SearchComponentUpdate;
import se.sics.ms.aggregator.SearchComponentUpdateSerializer;
import se.sics.ms.aggregator.data.ComponentUpdate;
import se.sics.ms.aggregator.data.SweepAggregatedPacket;
import se.sics.ms.aggregator.serializer.SweepPacketSerializer;
import se.sics.ms.election.aggregation.ElectionLeaderComponentUpdate;
import se.sics.ms.election.aggregation.ElectionLeaderUpdateSerializer;
import se.sics.ms.net.MessageFrameDecoder;
import se.sics.ms.serializer.SearchDescriptorSerializer;
import se.sics.ms.types.SearchDescriptor;
import se.sics.p2ptoolbox.aggregator.api.model.AggregatedStatePacket;
import se.sics.p2ptoolbox.aggregator.core.AggregatorNetworkSettings;
import se.sics.p2ptoolbox.croupier.api.util.PeerView;
import se.sics.p2ptoolbox.croupier.core.CroupierNetworkSettings;
import se.sics.p2ptoolbox.election.api.LCPeerView;
import se.sics.p2ptoolbox.election.core.LENetworkSettings;
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
    public static final byte AGGREGATED_STATE_PACKET_CODE = (byte) 0x03;
    public static final byte COMPONENT_UPDATE_ALIAS_CODE = (byte)0x04;
    public static final byte LCP_CODE = (byte)0x05;

    public static final String HEADER_FIELD_ALIAS = "SWEEP_HEADER_FIELD";
    public static final String PEER_VIEW_ALIAS = "SWEEP_PEER_VIEW";
    public static final String AGGREGATED_STATE_PACKET_ALIAS = "MY_STATE_PACKET";
    public static final String COMPONENT_UPDATE_ALIAS = "COMPONENT_UPDATE";

    public static final String LEADER_CAPABLE_PEER_VIEW = "LCP_VIEW";

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

            context.registerAlias(AggregatedStatePacket.class, AGGREGATED_STATE_PACKET_ALIAS, AGGREGATED_STATE_PACKET_CODE);
            context.registerSerializer(SweepAggregatedPacket.class, new SweepPacketSerializer());
            context.multiplexAlias(AGGREGATED_STATE_PACKET_ALIAS, SweepAggregatedPacket.class, (byte) 0x01);
            
            // Specific Component Serializer.

            context.registerAlias(ComponentUpdate.class, COMPONENT_UPDATE_ALIAS, COMPONENT_UPDATE_ALIAS_CODE);
            
            context.registerSerializer(SearchComponentUpdate.class, new SearchComponentUpdateSerializer());
            context.multiplexAlias(COMPONENT_UPDATE_ALIAS, SearchComponentUpdate.class, (byte) 0x01);
            
            context.registerSerializer(ElectionLeaderComponentUpdate.class, new ElectionLeaderUpdateSerializer());
            context.multiplexAlias(COMPONENT_UPDATE_ALIAS, ElectionLeaderComponentUpdate.class, (byte)0x02);

            // Leader Election Protocol Serializer.
            context.registerAlias(LCPeerView.class, LEADER_CAPABLE_PEER_VIEW, LCP_CODE);
            context.multiplexAlias(LEADER_CAPABLE_PEER_VIEW, SearchDescriptor.class, (byte)0x01);

        } catch (SerializationContext.DuplicateException ex) {
            throw new RuntimeException(ex);
        } catch (SerializationContext.MissingException ex) {
            throw new RuntimeException(ex);
        }
        
        CroupierNetworkSettings.oneTimeSetup(context, MessageFrameDecoder.CROUPIER_REQUEST, MessageFrameDecoder.CROUPIER_RESPONSE);
        GradientNetworkSettings.oneTimeSetup(context, MessageFrameDecoder.GRADIENT_REQUEST, MessageFrameDecoder.GRADIENT_RESPONSE);
        AggregatorNetworkSettings.oneTimeSetup(context, MessageFrameDecoder.AGGREGATOR_ONE_WAY);
        LENetworkSettings.oneTimeSetup(context, MessageFrameDecoder.LEADER_PROMISE_REQUEST, MessageFrameDecoder.LEADER_PROMISE_RESPONSE, MessageFrameDecoder.LEADER_EXTENSION_ONEWAY, MessageFrameDecoder.LEASE_COMMIT_ONEWAY);
    }
    
}
