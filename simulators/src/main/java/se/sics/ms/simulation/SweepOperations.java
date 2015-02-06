package se.sics.ms.simulation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.address.Address;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.msgs.DirectMsg;
import se.sics.kompics.Init;
import se.sics.kompics.p2p.experiment.dsl.adaptor.Operation;
import se.sics.kompics.p2p.experiment.dsl.adaptor.Operation1;
import se.sics.ms.simulation.PeerJoinP2pSimulated;
import se.sics.ms.simulator.P2pValidationMainInit;
import se.sics.ms.simulator.P2pValidatorMain;
import se.sics.p2ptoolbox.simulator.SimulationContext;
import se.sics.p2ptoolbox.simulator.cmd.NetworkOpCmd;
import se.sics.p2ptoolbox.simulator.cmd.OperationCmd;
import se.sics.p2ptoolbox.simulator.cmd.StartNodeCmd;

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
            InetAddress localHost = InetAddress.getLocalHost();
            nodeAddress = new VodAddress(new Address(localHost,defaultPort,defaultNodeId), defaultOverlayAddress);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }
    
    
    public static Operation<StartNodeCmd> startNodeCmdOperation = new Operation<StartNodeCmd>() {
        @Override
        public StartNodeCmd generate() {
            
            return new StartNodeCmd<P2pValidatorMain>() {

                @Override
                public Integer getNodeId() {
                    return defaultNodeId;
                }

                @Override
                public Class getNodeComponentDefinition() {
                    return P2pValidatorMain.class;
                }

                @Override
                public P2pValidationMainInit getNodeComponentInit() {
                    return new P2pValidationMainInit(3000, 2 , 1.0d , nodeAddress);
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
                    logger.info("Trying to fetch the network message .... ");
                    logger.info("Origin : " + origin.toString());
                    logger.info("My Node Address : " + nodeAddress.toString());
                            
                    return new PeerJoinP2pSimulated.Request(origin, nodeAddress, peerId);
                }

                @Override
                public void beforeCmd(SimulationContext simulationContext) {
                    // Executed before the command is executed.
                    logger.info("Executing command to start peer with Id: " + peerId);
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

}
