package se.sics.ms.simulation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.cm.ChunkManagerConfiguration;
import se.sics.gvod.address.Address;
import se.sics.gvod.config.*;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.msgs.DirectMsg;
import se.sics.kompics.p2p.experiment.dsl.adaptor.Operation;
import se.sics.kompics.p2p.experiment.dsl.adaptor.Operation1;
import se.sics.ms.configuration.MsConfig;
import se.sics.ms.simulator.P2pSim;
import se.sics.ms.simulator.P2pSimulatorInit;
import se.sics.p2ptoolbox.croupier.api.CroupierSelectionPolicy;
import se.sics.p2ptoolbox.croupier.core.CroupierConfig;
import se.sics.p2ptoolbox.simulator.SimulationContext;
import se.sics.p2ptoolbox.simulator.cmd.NetworkOpCmd;
import se.sics.p2ptoolbox.simulator.cmd.OperationCmd;
import se.sics.p2ptoolbox.simulator.cmd.StartNodeCmd;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by babbarshaer on 2015-02-04.
 */
public class SweepOperations {

    private static Logger logger = LoggerFactory.getLogger(SweepOperations.class);
    
    private static VodAddress nodeAddress;
    private static int defaultPort = 12345;
    private static int defaultNodeId = 100;
    private static int defaultOverlayAddress = 0;
    
    static{
        
        // Create default address of InetAddress.
        try {
            // === Initialize VodConfig.
            VodConfig.init(new String[0]);
            InetAddress localHost = InetAddress.getLocalHost();
            
            nodeAddress = new VodAddress(new Address(localHost,defaultPort,defaultNodeId), defaultOverlayAddress);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    
    public static Operation<StartNodeCmd> startNodeCmdOperation = new Operation<StartNodeCmd>() {
            
            public StartNodeCmd generate(){
                
                return new StartNodeCmd<P2pSim>() {


                    CroupierSelectionPolicy hardcodedPolicy = CroupierSelectionPolicy.RANDOM;
                    CroupierConfig croupierConfig = new CroupierConfig(MsConfig.CROUPIER_VIEW_SIZE, MsConfig.CROUPIER_SHUFFLE_PERIOD,
                            MsConfig.CROUPIER_SHUFFLE_LENGTH, hardcodedPolicy);

                    @Override
                    public Integer getNodeId() {
                        return defaultNodeId;
                    }

                    @Override
                    public Class getNodeComponentDefinition() {
                        return P2pSim.class;
                    }

                    @Override
                    public P2pSimulatorInit getNodeComponentInit(VodAddress statusServer) {
                        return new P2pSimulatorInit(nodeAddress, statusServer, croupierConfig, GradientConfiguration.build(), SearchConfiguration.build()
                                                    , ElectionConfiguration.build(), ChunkManagerConfiguration.build());
                    }
                };
            }
    };
    
    // Instantiate Peers.
    public static Operation1<NetworkOpCmd, Long> peerJoinCommand = new Operation1<NetworkOpCmd, Long>(){

        @Override
        public NetworkOpCmd generate(final Long peerId) {
            
            return new NetworkOpCmd() {
                
                @Override
                public DirectMsg getNetworkMsg(VodAddress origin) {
                    logger.debug("Peer join operation invoked for Id -> " + peerId);
                    return new PeerJoinP2pSimulated.Request(origin, nodeAddress, peerId);
                }

                @Override
                public void beforeCmd(SimulationContext simulationContext) {
                }

                @Override
                public boolean myResponse(DirectMsg response) {
                    return true;
                }

                @Override
                public void validate(SimulationContext simulationContext, DirectMsg directMsg) throws ValidationException {
                    // Validator in here.
                }

                @Override
                public void afterValidation(SimulationContext simulationContext) {
                    // Executed after validation.
                }
            };
            
        }
    };
    
    
    public static Operation1<NetworkOpCmd,Long> addIndexEntryCommand = new Operation1<NetworkOpCmd, Long>() {
        @Override
        public NetworkOpCmd generate(final Long id) {
            
            return new NetworkOpCmd() {
                @Override
                public DirectMsg getNetworkMsg(VodAddress origin) {
                    logger.debug("Add Index Entry id invoked for id -> " + id);
                    return new IndexEntryP2pSimulated.Request(origin, nodeAddress, id);
                }

                @Override
                public void beforeCmd(SimulationContext context) {

                }

                @Override
                public boolean myResponse(DirectMsg response) {
                    return true;
                }

                @Override
                public void validate(SimulationContext context, DirectMsg response) throws ValidationException {

                }

                @Override
                public void afterValidation(SimulationContext context) {

                }
            };
        }
    };

}
