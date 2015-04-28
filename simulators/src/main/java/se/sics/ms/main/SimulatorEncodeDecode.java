package se.sics.ms.main;

/**
 * Created by babbarshaer on 2015-03-20.
 */
public class SimulatorEncodeDecode {

/*

    //other aliases
    public static final byte HEADER_FIELD_CODE = (byte) 0x01;
    public static final byte PEER_VIEW_CODE = (byte) 0x02;
    public static final byte AGGREGATED_STATE_PACKET_CODE = (byte) 0x03;

    public static final String HEADER_FIELD_ALIAS = "SWEEP_HEADER_FIELD";
    public static final String PEER_VIEW_ALIAS = "SWEEP_PEER_VIEW";
    public static final String AGGREGATED_STATE_PACKET_ALIAS = "MY_STATE_PACKET";

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
            context.registerSerializer(SearchDescriptor.class, new SearchDescriptorSerializerOld());
            context.multiplexAlias(PEER_VIEW_ALIAS, SearchDescriptor.class, (byte)0x01);

            context.registerAlias(AggregatedStatePacket.class, AGGREGATED_STATE_PACKET_ALIAS, AGGREGATED_STATE_PACKET_CODE);
            context.registerSerializer(SweepAggregatedPacket.class, new SweepPacketSerializer());
            context.multiplexAlias(AGGREGATED_STATE_PACKET_ALIAS, SweepAggregatedPacket.class, (byte) 0x01);

            // Specific Component Serializer.
            context.registerSerializer(SearchComponentUpdate.class, new SearchComponentUpdateSerializer());
            context.multiplexAlias(AGGREGATED_STATE_PACKET_ALIAS, SearchComponentUpdate.class, (byte) 0x02);

            context.registerSerializer(ElectionLeaderComponentUpdate.class, new ElectionLeaderUpdateSerializer());
            context.multiplexAlias(AGGREGATED_STATE_PACKET_ALIAS, ElectionLeaderComponentUpdate.class, (byte)0x03);


        } catch (SerializationContext.DuplicateException ex) {
            throw new RuntimeException(ex);
        } catch (SerializationContext.MissingException ex) {
            throw new RuntimeException(ex);
        }

        CroupierNetworkSettings.oneTimeSetup(context, MessageFrameDecoder.CROUPIER_REQUEST, MessageFrameDecoder.CROUPIER_RESPONSE);
        GradientNetworkSettings.oneTimeSetup(context, MessageFrameDecoder.GRADIENT_REQUEST, MessageFrameDecoder.GRADIENT_RESPONSE);
        AggregatorNetworkSettings.oneTimeSetup(context, MessageFrameDecoder.AGGREGATOR_ONE_WAY);
    }
    
    
  */
}
