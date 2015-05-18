package se.sics.ms.simulation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.config.VodConfig;
import se.sics.kompics.Init;
import se.sics.kompics.KompicsEvent;
import se.sics.kompics.network.Address;
import se.sics.kompics.network.Msg;
import se.sics.kompics.network.Transport;
import se.sics.ms.events.simEvents.AddIndexEntryP2pSimulated;
import se.sics.ms.events.simEvents.SearchP2pSimulated;
import se.sics.ms.net.SerializerSetup;
import se.sics.ms.search.SearchPeer;
import se.sics.ms.search.SearchPeerInit;
import se.sics.ms.types.IndexEntry;
import se.sics.ms.types.SearchPattern;
import se.sics.p2ptoolbox.aggregator.network.AggregatorSerializerSetup;
import se.sics.p2ptoolbox.chunkmanager.ChunkManagerSerializerSetup;
import se.sics.p2ptoolbox.croupier.CroupierSerializerSetup;
import se.sics.p2ptoolbox.election.network.ElectionSerializerSetup;
import se.sics.p2ptoolbox.gradient.GradientSerializerSetup;
import se.sics.p2ptoolbox.simulator.SimulationContext;
import se.sics.p2ptoolbox.simulator.cmd.NetworkOpCmd;
import se.sics.p2ptoolbox.simulator.cmd.impl.StartNodeCmd;
import se.sics.p2ptoolbox.simulator.dsl.adaptor.Operation1;
import se.sics.p2ptoolbox.util.network.impl.BasicAddress;
import se.sics.p2ptoolbox.util.network.impl.BasicContentMsg;
import se.sics.p2ptoolbox.util.network.impl.DecoratedAddress;
import se.sics.p2ptoolbox.util.network.impl.DecoratedHeader;
import se.sics.p2ptoolbox.util.serializer.BasicSerializerSetup;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Set;

/**
 * Operations for controlling the sequence of events in sweep.
 *
 * Created by babbarshaer on 2015-02-04.
 */
public class SweepOperations {

    private static Logger logger = LoggerFactory.getLogger(SweepOperations.class);

    static{
        
        try {
            
            VodConfig.init(new String[0]);

            int currentId = 128;                                        // Serializers Registration.
            BasicSerializerSetup.registerBasicSerializers(currentId);
            currentId += BasicSerializerSetup.serializerIds;
            currentId = CroupierSerializerSetup.registerSerializers(currentId);
            currentId = GradientSerializerSetup.registerSerializers(currentId);
            currentId = ElectionSerializerSetup.registerSerializers(currentId);
            currentId = AggregatorSerializerSetup.registerSerializers(currentId);
            currentId = ChunkManagerSerializerSetup.registerSerializers(currentId);
            SerializerSetup.registerSerializers(currentId);
        } 
        catch (UnknownHostException e) {
            e.printStackTrace();
        } 
        catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    
    public static Operation1<StartNodeCmd, Long> startNodeCmdOperation = new Operation1<StartNodeCmd, Long>() {
        @Override
        public StartNodeCmd generate(final Long id) {

            return new StartNodeCmd<SearchPeer, DecoratedAddress>() {

                long nodeId = SweepOperationsHelper.getStableId(id);

                @Override
                public Integer getNodeId() {
                    return (int)nodeId;
                }

                @Override
                public int bootstrapSize() {
                    return 2;
                }

                @Override
                public Class getNodeComponentDefinition() {
                    return SearchPeer.class;
                }

                @Override
                public SearchPeerInit getNodeComponentInit(DecoratedAddress address, Set<DecoratedAddress> bootstrapNodes) {
                    return SweepOperationsHelper.generatePeerInit(address, bootstrapNodes, nodeId);
                }

                @Override
                public DecoratedAddress  getAddress() {
                    return SweepOperationsHelper.getBasicAddress(nodeId);
                }
            };
        }
    };
    
    
    public static Operation1<NetworkOpCmd,Long> addIndexEntryCommand = new Operation1<NetworkOpCmd, Long>() {

        @Override
        public NetworkOpCmd generate(final Long id) {
            return new NetworkOpCmd(){

                DecoratedAddress destination = SweepOperationsHelper.getNodeAddressToCommunicate(id);
                IndexEntry junkEntry = SweepOperationsHelper.generateIndexEntry();

                @Override
                public void beforeCmd(SimulationContext simulationContext) {

                }

                @Override
                public boolean myResponse(KompicsEvent kompicsEvent) {
                    return false;
                }

                @Override
                public void validate(SimulationContext simulationContext, KompicsEvent kompicsEvent) throws ValidationException {

                }

                @Override
                public void afterValidation(SimulationContext simulationContext) {

                }

                @Override
                public Msg getNetworkMsg(Address address) {

                    logger.debug("Add Index Entry id invoked for id -> " + destination.getId());

                    AddIndexEntryP2pSimulated request = new AddIndexEntryP2pSimulated(junkEntry);
                    DecoratedHeader<DecoratedAddress> header = new DecoratedHeader<DecoratedAddress>((DecoratedAddress)address, destination, Transport.UDP);
                    BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, AddIndexEntryP2pSimulated> msg = new BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, AddIndexEntryP2pSimulated>(header, request);
                    return msg;
                }
            };
        }
    };


    public static Operation1<NetworkOpCmd,Long> searchIndexEntry = new Operation1<NetworkOpCmd, Long>() {

        @Override
        public NetworkOpCmd generate(final Long id) {
            return new NetworkOpCmd(){

                DecoratedAddress destination = SweepOperationsHelper.getNodeAddressToCommunicate(id);
                SearchPattern pattern = SweepOperationsHelper.generateSearchPattern();

                @Override
                public void beforeCmd(SimulationContext simulationContext) {

                }

                @Override
                public boolean myResponse(KompicsEvent kompicsEvent) {
                    return false;
                }

                @Override
                public void validate(SimulationContext simulationContext, KompicsEvent kompicsEvent) throws ValidationException {

                }

                @Override
                public void afterValidation(SimulationContext simulationContext) {

                }

                @Override
                public Msg getNetworkMsg(Address address) {

                    logger.debug("Search Index Entry Command invoked for ->" + destination.getId());

                    SearchP2pSimulated simulated = new SearchP2pSimulated(pattern);
                    DecoratedHeader<DecoratedAddress> header = new DecoratedHeader<DecoratedAddress>((DecoratedAddress)address, destination, Transport.UDP);
                    BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, SearchP2pSimulated> msg = new BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, SearchP2pSimulated>(header, simulated);
                    
                    return msg;
                }
            };
        }
    };
    
    
    
    
}
