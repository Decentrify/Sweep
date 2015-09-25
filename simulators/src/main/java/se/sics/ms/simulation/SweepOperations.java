package se.sics.ms.simulation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.Init;
import se.sics.kompics.KompicsEvent;
import se.sics.kompics.network.Address;
import se.sics.kompics.network.Msg;
import se.sics.kompics.network.Transport;
import se.sics.ktoolbox.cc.sim.CCSimMain;
import se.sics.ktoolbox.cc.sim.CCSimMainInit;
import se.sics.ms.configuration.MsConfig;
import se.sics.ms.events.simEvents.AddIndexEntryP2pSimulated;
import se.sics.ms.events.simEvents.SearchP2pSimulated;
import se.sics.ms.main.*;
import se.sics.ms.types.IndexEntry;
import se.sics.ms.types.SearchPattern;
import se.sics.p2ptoolbox.simulator.SimulationContext;
import se.sics.p2ptoolbox.simulator.cmd.NetworkOpCmd;
import se.sics.p2ptoolbox.simulator.cmd.OperationCmd;
import se.sics.p2ptoolbox.simulator.cmd.impl.ChangeNetworkModelCmd;
import se.sics.p2ptoolbox.simulator.cmd.impl.KillNodeCmd;
import se.sics.p2ptoolbox.simulator.cmd.impl.StartAggregatorCmd;
import se.sics.p2ptoolbox.simulator.cmd.impl.StartNodeCmd;
import se.sics.p2ptoolbox.simulator.core.network.impl.BasicLossyLinkModel;
import se.sics.p2ptoolbox.simulator.core.network.impl.agg.UniformRandomModel;
import se.sics.p2ptoolbox.simulator.dsl.adaptor.*;
import se.sics.p2ptoolbox.util.network.impl.BasicContentMsg;
import se.sics.p2ptoolbox.util.network.impl.DecoratedAddress;
import se.sics.p2ptoolbox.util.network.impl.DecoratedHeader;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;
import java.util.Set;

/**
 * Operations for controlling the sequence of events in sweep.
 * <p/>
 * Created by babbarshaer on 2015-02-04.
 */
public class SweepOperations {

    private static Logger logger = LoggerFactory.getLogger(SweepOperations.class);
    private static FileWriter writer = null;
    static {
    }
    
    
    
    public static Operation<ChangeNetworkModelCmd> uniformNetworkModel = new Operation<ChangeNetworkModelCmd>() {
        @Override
        public ChangeNetworkModelCmd generate() {
            return new ChangeNetworkModelCmd(new UniformRandomModel(50, 250));
        }
    };


    /**
     * Operation used to generate the basic lossy link model
     * with a uniform random model to fall back on.
     */
    public static Operation1<ChangeNetworkModelCmd, Long> lossyLinkModel = new Operation1<ChangeNetworkModelCmd, Long>() {
        @Override
        public ChangeNetworkModelCmd generate(Long lossRate) {

            Random random = new Random(1);
            UniformRandomModel baseModel = new UniformRandomModel(50, 250, random) ;
            return new ChangeNetworkModelCmd(new BasicLossyLinkModel(1, baseModel, lossRate.intValue(), random));

        }
    };
    
    
    public static Operation1<StartNodeCmd, Long> startLeaderGroupNodes = new Operation1<StartNodeCmd, Long>() {
        @Override
        public StartNodeCmd generate(final Long id) {

            return new StartNodeCmd<SimulatorHostComp, DecoratedAddress>() {

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
                    return SimulatorHostComp.class;
                }

                @Override
                public SimulatorHostCompInit getNodeComponentInit(DecoratedAddress address, Set<DecoratedAddress> bootstrapNodes) {
                    return SweepOperationsHelper.getSimHostCompInit(address, bootstrapNodes, nodeId);
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

            return new StartNodeCmd<SimulatorHostComp, DecoratedAddress>() {

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
                    return SimulatorHostComp.class;
                }

                @Override
                public SimulatorHostCompInit getNodeComponentInit(DecoratedAddress address, Set<DecoratedAddress> bootstrapNodes) {
                    return SweepOperationsHelper.getSimHostCompInit(address, bootstrapNodes, nodeId);
                }

                @Override
                public DecoratedAddress  getAddress() {
                    return SweepOperationsHelper.getBasicAddress(nodeId);
                }
            };
        }
    };


    public static Operation1<StartNodeCmd, Long> startPALNodeCmdOperation = new Operation1<StartNodeCmd, Long>() {
        @Override
        public StartNodeCmd generate(final Long id) {

            return new StartNodeCmd<SimulatorHostComp, DecoratedAddress>() {

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
                    return SimulatorHostComp.class;
                }

                @Override
                public SimulatorHostCompInit getNodeComponentInit(DecoratedAddress address, Set<DecoratedAddress> bootstrapNodes) {
                    return SweepOperationsHelper.getSimHostCompInit(address, bootstrapNodes, nodeId);
                }

                @Override
                public DecoratedAddress  getAddress() {
                    return SweepOperationsHelper.getBasicAddress(nodeId);
                }
            };
        }
    };
    


    public static Operation1<StartNodeCmd, Long> startSimulationHostComp = new Operation1<StartNodeCmd, Long>() {
        @Override
        public StartNodeCmd generate(final Long id) {
            return new StartNodeCmd<SimulatorHostComp, DecoratedAddress>() {

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
                public Class<SimulatorHostComp> getNodeComponentDefinition() {
                    return SimulatorHostComp.class;
                }

                @Override
                public SimulatorHostCompInit getNodeComponentInit(DecoratedAddress address, Set<DecoratedAddress> set) {
                    return SweepOperationsHelper.getSimHostCompInit(address, set, nodeId);
                }

                @Override
                public DecoratedAddress getAddress() {
                    return SweepOperationsHelper.getPeerAddress(nodeId);
                }
            };
        }
    };


    public static Operation1<StartNodeCmd, Long> startCaracalClient = new Operation1<StartNodeCmd, Long>() {
        @Override
        public StartNodeCmd generate(final Long id) {

            return new StartNodeCmd<CCSimMain, DecoratedAddress>(){

                long nodeId = SweepOperationsHelper.getStableId(id);

                @Override
                public Integer getNodeId() {
                    return (int)nodeId;
                }

                @Override
                public int bootstrapSize() {
                    return 0;
                }

                @Override
                public Class<CCSimMain> getNodeComponentDefinition() {
                    return CCSimMain.class;
                }

                @Override
                public CCSimMainInit getNodeComponentInit(DecoratedAddress address, Set<DecoratedAddress> set) {
                    return SweepOperationsHelper.getCCSimInit(nodeId);
                }

                @Override
                public DecoratedAddress getAddress() {

//                  Before returning, store the address.

                    DecoratedAddress ccSimAddress = SweepOperationsHelper.getPeerAddress(nodeId);
                    SweepOperationsHelper.storeCaracalSimClientAddress(ccSimAddress);

                    return ccSimAddress;
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
    



	public static Operation1<StartNodeCmd, Long> startPAGNodeCmdOperation = new Operation1<StartNodeCmd, Long>() {
        @Override
        public StartNodeCmd generate(final Long id) {

            return new StartNodeCmd<SimulatorHostComp, DecoratedAddress>() {

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
                    return SimulatorHostComp.class;
                }

                @Override
                public SimulatorHostCompInit getNodeComponentInit(DecoratedAddress address, Set<DecoratedAddress> bootstrapNodes) {
                    return SweepOperationsHelper.getSimHostCompInit(address, bootstrapNodes, nodeId);
                }

                @Override
                public DecoratedAddress  getAddress() {
                    return SweepOperationsHelper.getBasicAddress(nodeId);
                }
            };
        }
    };
    

    public static Operation1<StartNodeCmd, Long> startPartitionNodeCmd = new Operation1<StartNodeCmd, Long>() {
        @Override
        public StartNodeCmd generate(final Long id) {

            return new StartNodeCmd<SimulatorHostComp, DecoratedAddress>() {

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
                    return SimulatorHostComp.class;
                }

                @Override
                public SimulatorHostCompInit getNodeComponentInit(DecoratedAddress address, Set<DecoratedAddress> bootstrapNodes) {
                    return SweepOperationsHelper.getSimHostCompInit(address, bootstrapNodes, nodeId);
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

                    logger.error("Add Index Entry id invoked for id -> " + destination.getId());
                    AddIndexEntryP2pSimulated request = new AddIndexEntryP2pSimulated(junkEntry);
                    DecoratedHeader<DecoratedAddress> header = new DecoratedHeader<DecoratedAddress>((DecoratedAddress) address, destination, Transport.UDP);
                    BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, AddIndexEntryP2pSimulated> msg = new BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, AddIndexEntryP2pSimulated>(header, request);
                    return msg;
                }
            };
        }
    };


    public static Operation4<NetworkOpCmd, Long, Long, Long,Long> bucketAwareSearchEntry = new Operation4<NetworkOpCmd, Long, Long, Long, Long>() {

        @Override
        public NetworkOpCmd generate(final Long bucketId, final Long searchTimeout, final Long fanoutParameter, final Long expectedEntries) {
            
            return new NetworkOpCmd() {

                DecoratedAddress destination = SweepOperationsHelper.getRandomNodeForBucket(bucketId);
                SearchPattern pattern = SweepOperationsHelper.generateSearchPattern();
                
                long startTime;
                
                @Override
                public void beforeCmd(SimulationContext simulationContext) {
                    startTime = System.currentTimeMillis();
                }

                @Override
                public boolean myResponse(KompicsEvent kompicsEvent) {
                    
                    if(kompicsEvent instanceof  BasicContentMsg){
                        BasicContentMsg msg = (BasicContentMsg)kompicsEvent;
                        return (msg.getContent() != null && msg.getContent() instanceof SearchP2pSimulated.Response);
                    }
                    return false;
                }

                @Override
                public void validate(SimulationContext simulationContext, KompicsEvent kompicsEvent) throws ValidationException {
                    
                    try {
                        SearchP2pSimulated.Response response = (SearchP2pSimulated.Response)(((BasicContentMsg)kompicsEvent).getContent());
                        logger.error("Simulator Received the response from the container component.");
                        logger.error("Partition Hit: {}, Responses:{}", response.getPartitionHit(), response.getResponses());
                        writer.write(response.getPartitionHit() +"," + expectedEntries +"," +response.getResponses() +"\n");
                        writer.flush();
                    } catch (IOException e) {
                        e.printStackTrace();
                        throw new RuntimeException("Unable to log the results.");
                    }
                }

                @Override
                public void afterValidation(SimulationContext simulationContext) {
                    // Log the Result in the class.
                    logger.error("Will log the search time results.");
                }

                @Override
                public Msg getNetworkMsg(Address address) {

                    logger.error("Search Index Entry Command invoked for ->" + destination.getId());
                    SearchP2pSimulated.Request simulated = new SearchP2pSimulated.Request(pattern, searchTimeout.intValue(), fanoutParameter.intValue());
                    DecoratedHeader<DecoratedAddress> header = new DecoratedHeader<DecoratedAddress>((DecoratedAddress) address, destination, Transport.UDP);
                    BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, SearchP2pSimulated.Request> msg = new BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, SearchP2pSimulated.Request>(header, simulated);

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
                public Msg getNetworkMsg (Address address) {

                    logger.debug("Search Index Entry Command invoked for ->" + destination.getId());
                    SearchP2pSimulated.Request simulated = new SearchP2pSimulated.Request(pattern, searchTimeout.intValue(), fanoutParameter.intValue());
                    DecoratedHeader<DecoratedAddress> header = new DecoratedHeader<DecoratedAddress>((DecoratedAddress) address, destination, Transport.UDP);
                    BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, SearchP2pSimulated.Request> msg = new BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, SearchP2pSimulated.Request>(header, simulated);

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


    /**
     * Main method to start the aggregator node in the system.
     * The simulator keeps track of the aggregator node and supplies the information to
     * other nodes in the system.
     *
     */
    public static Operation<StartAggregatorCmd<AggregatorHostComp, DecoratedAddress>> getAggregatorComponent(final TerminateConditionWrapper terminateCondition){

        Operation<StartAggregatorCmd<AggregatorHostComp, DecoratedAddress>> result = new Operation<StartAggregatorCmd<AggregatorHostComp, DecoratedAddress>>() {
            @Override
            public StartAggregatorCmd<AggregatorHostComp, DecoratedAddress> generate() {
                return new StartAggregatorNode(MsConfig.AGGREGATOR_TIMEOUT, MsConfig.SIMULATION_FILE_LOC, terminateCondition);
            }
        };

        return result;
    }
    
}
