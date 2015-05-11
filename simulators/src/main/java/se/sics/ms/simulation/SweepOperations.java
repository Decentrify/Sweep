package se.sics.ms.simulation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.Init;
import se.sics.kompics.KompicsEvent;
import se.sics.kompics.network.Address;
import se.sics.kompics.network.Msg;
import se.sics.kompics.network.Transport;
import se.sics.ms.events.simEvents.AddIndexEntryP2pSimulated;
import se.sics.ms.events.simEvents.SearchP2pSimulated;
import se.sics.ms.launch.global.aggregator.main.SystemAggregatorApplication;
import se.sics.ms.launch.global.aggregator.main.SystemAggregatorApplicationInit;
import se.sics.ms.search.SearchPeer;
import se.sics.ms.search.SearchPeerInit;
import se.sics.ms.types.IndexEntry;
import se.sics.ms.types.SearchPattern;
import se.sics.p2ptoolbox.simulator.SimulationContext;
import se.sics.p2ptoolbox.simulator.cmd.NetworkOpCmd;
import se.sics.p2ptoolbox.simulator.cmd.OperationCmd;
import se.sics.p2ptoolbox.simulator.cmd.impl.KillNodeCmd;
import se.sics.p2ptoolbox.simulator.cmd.impl.StartAggregatorCmd;
import se.sics.p2ptoolbox.simulator.cmd.impl.StartNodeCmd;
import se.sics.p2ptoolbox.simulator.dsl.adaptor.Operation;
import se.sics.p2ptoolbox.simulator.dsl.adaptor.Operation1;
import se.sics.p2ptoolbox.simulator.dsl.adaptor.Operation2;
import se.sics.p2ptoolbox.simulator.dsl.adaptor.Operation3;
import se.sics.p2ptoolbox.util.network.impl.BasicContentMsg;
import se.sics.p2ptoolbox.util.network.impl.DecoratedAddress;
import se.sics.p2ptoolbox.util.network.impl.DecoratedHeader;
import java.util.Set;

/**
 * Operations for controlling the sequence of events in sweep.
 * <p/>
 * Created by babbarshaer on 2015-02-04.
 */
public class SweepOperations {

    private static Logger logger = LoggerFactory.getLogger(SweepOperations.class);

    
    public static Operation<StartAggregatorCmd<SystemAggregatorApplication, DecoratedAddress>> startAggregatorNodeCmd =
            new Operation<StartAggregatorCmd<SystemAggregatorApplication, DecoratedAddress>>() {

                @Override
                public StartAggregatorCmd<SystemAggregatorApplication, DecoratedAddress> generate() {
                    final DecoratedAddress aggregatorAddress = SweepOperationsHelper.getAggregatorAddress();
                    
                    return new StartAggregatorCmd<SystemAggregatorApplication, DecoratedAddress>() {

                        @Override
                        public Class<SystemAggregatorApplication> getNodeComponentDefinition() {
                            return SystemAggregatorApplication.class;
                        }

                        @Override
                        public Init getNodeComponentInit() {
                            return new SystemAggregatorApplicationInit(null);
                        }

                        @Override
                        public DecoratedAddress getAddress() {
                            return aggregatorAddress;
                        }
                    };
                }
    };



    public static Operation1<StartNodeCmd, Long> startLeaderGroupNodes = new Operation1<StartNodeCmd, Long>() {
        @Override
        public StartNodeCmd generate(final Long id) {

            return new StartNodeCmd<SearchPeer, DecoratedAddress>() {

                long nodeId = SweepOperationsHelper.getLeaderGroupNodeId(id);

                @Override
                public Integer getNodeId() {
                    return (int) nodeId;
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
                public DecoratedAddress getAddress() {
                    return SweepOperationsHelper.getBasicAddress(nodeId);
                }
            };
        }
    };
    
    
    
    
    
    
    public static Operation1<StartNodeCmd, Long> startNodeCmdOperation = new Operation1<StartNodeCmd, Long>() {
        @Override
        public StartNodeCmd generate(final Long id) {

            return new StartNodeCmd<SearchPeer, DecoratedAddress>() {

                long nodeId = SweepOperationsHelper.getStableId(id);

                @Override
                public Integer getNodeId() {
                    return (int) nodeId;
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
                public DecoratedAddress getAddress() {
                    return SweepOperationsHelper.getBasicAddress(nodeId);
                }
            };
        }
    };


    public static Operation2<OperationCmd, Long, Long> generatePartitionNodeMap = new Operation2<OperationCmd, Long, Long>() {
        
        @Override
        public OperationCmd generate(final Long depth, final Long bucketSize) {
            
            SweepOperationsHelper.generateNodesPerPartition( depth, bucketSize, 9 );
            
            return new OperationCmd() {
                
                @Override
                public void beforeCmd(SimulationContext context) {
                    
                }

                @Override
                public boolean myResponse(KompicsEvent response) {
                    return false;
                }

                @Override
                public void validate(SimulationContext context, KompicsEvent response) throws ValidationException {

                }

                @Override
                public void afterValidation(SimulationContext context) {

                }
            };
        }
    };
    
    

    public static Operation1<StartNodeCmd, Long> startPartitionNodeCmd = new Operation1<StartNodeCmd, Long>() {
        @Override
        public StartNodeCmd generate(final Long id) {

            return new StartNodeCmd<SearchPeer, DecoratedAddress>() {

                long nodeId = SweepOperationsHelper.getPartitionBucketNode(id);

                @Override
                public Integer getNodeId() {
                    return (int) nodeId;
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
                public DecoratedAddress getAddress() {
                    return SweepOperationsHelper.getBasicAddress(nodeId);
                }
            };
        }
    };



    public static Operation1<NetworkOpCmd, Long> addPartitionIndexEntryCommand = new Operation1<NetworkOpCmd, Long>() {

        @Override
        public NetworkOpCmd generate(final Long id) {
            return new NetworkOpCmd() {

                DecoratedAddress destination = SweepOperationsHelper.getBucketNodeToAddEntry(id);
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

                    logger.debug("Add Index Entry id invoked for id -> " + id);

                    AddIndexEntryP2pSimulated request = new AddIndexEntryP2pSimulated(junkEntry);
                    DecoratedHeader<DecoratedAddress> header = new DecoratedHeader<DecoratedAddress>((DecoratedAddress) address, destination, Transport.UDP);
                    BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, AddIndexEntryP2pSimulated> msg = new BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, AddIndexEntryP2pSimulated>(header, request);
                    return msg;
                }
            };
        }
    };






    public static Operation1<NetworkOpCmd, Long> addBucketAwareEntry = new Operation1<NetworkOpCmd, Long>() {

        @Override
        public NetworkOpCmd generate(final Long bucketId) {
            return new NetworkOpCmd() {

                DecoratedAddress destination = SweepOperationsHelper.getNodeForBucket(bucketId);
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
                    DecoratedHeader<DecoratedAddress> header = new DecoratedHeader<DecoratedAddress>((DecoratedAddress) address, destination, Transport.UDP);
                    BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, AddIndexEntryP2pSimulated> msg = new BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, AddIndexEntryP2pSimulated>(header, request);
                    return msg;
                }
            };
        }
    };
    
    
    


    public static Operation1<NetworkOpCmd, Long> addIndexEntryCommand = new Operation1<NetworkOpCmd, Long>() {

        @Override
        public NetworkOpCmd generate(final Long id) {
            return new NetworkOpCmd() {

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

                    logger.debug("Add Index Entry id invoked for id -> " + id);

                    AddIndexEntryP2pSimulated request = new AddIndexEntryP2pSimulated(junkEntry);
                    DecoratedHeader<DecoratedAddress> header = new DecoratedHeader<DecoratedAddress>((DecoratedAddress) address, destination, Transport.UDP);
                    BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, AddIndexEntryP2pSimulated> msg = new BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, AddIndexEntryP2pSimulated>(header, request);
                    return msg;
                }
            };
        }
    };


    public static Operation3<NetworkOpCmd, Long, Long, Long> bucketAwareSearchEntry = new Operation3<NetworkOpCmd, Long, Long, Long>() {

        @Override
        public NetworkOpCmd generate(final Long bucketId, final Long searchTimeout, final Long fanoutParameter) {
            
            return new NetworkOpCmd() {

                DecoratedAddress destination = SweepOperationsHelper.getNodeForBucket(bucketId);
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

                    logger.error("Search Index Entry Command invoked for ->" + destination.getId());
                    SearchP2pSimulated simulated = new SearchP2pSimulated(pattern, searchTimeout.intValue(), fanoutParameter.intValue());
                    DecoratedHeader<DecoratedAddress> header = new DecoratedHeader<DecoratedAddress>((DecoratedAddress) address, destination, Transport.UDP);
                    BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, SearchP2pSimulated> msg = new BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, SearchP2pSimulated>(header, simulated);

                    return msg;
                }
            };
        }
    };
    
    
    
    

    public static Operation3<NetworkOpCmd, Long, Long, Long> searchIndexEntry = new Operation3<NetworkOpCmd, Long, Long, Long>() {

        @Override
        public NetworkOpCmd generate(final Long id, final Long searchTimeout, final Long fanoutParameter) {
            return new NetworkOpCmd() {

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

                    SearchP2pSimulated simulated = new SearchP2pSimulated(pattern, searchTimeout.intValue(), fanoutParameter.intValue());
                    DecoratedHeader<DecoratedAddress> header = new DecoratedHeader<DecoratedAddress>((DecoratedAddress) address, (DecoratedAddress) address, Transport.UDP);
                    BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, SearchP2pSimulated> msg = new BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, SearchP2pSimulated>(header, simulated);

                    return msg;
                }
            };
        }
    };


    /**
     * Kill the node command operation, 
     * in which the node fetches the node id to kill the node.
     *
     */
    public static Operation1<KillNodeCmd, Long> killNodeCmdOperation = new Operation1<KillNodeCmd, Long>() {

        @Override
        public KillNodeCmd generate(final Long id) {
            
            return new KillNodeCmd() {
                
                Integer nodeId = SweepOperationsHelper.removeNode(id);
                
                @Override
                public Integer getNodeId() {

                    System.out.println(" Node To Kill : " + nodeId);
                    return nodeId;
                }
            };
        }
    };
    
    
    

}
