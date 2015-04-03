package se.sics.p2ptoolbox.election.example.simulator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.msgs.DirectMsg;
import se.sics.kompics.p2p.experiment.dsl.adaptor.Operation;
import se.sics.kompics.p2p.experiment.dsl.adaptor.Operation1;
import se.sics.p2ptoolbox.election.example.main.HostManagerComp;
import se.sics.p2ptoolbox.election.example.msg.AddPeers;
import se.sics.p2ptoolbox.simulator.SimulationContext;
import se.sics.p2ptoolbox.simulator.cmd.NetworkOpCmd;
import se.sics.p2ptoolbox.simulator.cmd.StartNodeCmd;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Operations for testing the leader election algorithm.
 *
 * Created by babbar on 2015-04-01.
 */
public class LeaderElectionOperations {

    private static Logger logger = LoggerFactory.getLogger(LeaderElectionOperations.class);

    public static Operation1<StartNodeCmd, Long> startHostManager = new Operation1<StartNodeCmd, Long>() {

        public StartNodeCmd generate(final Long id){

            return new StartNodeCmd<HostManagerComp>() {

                long nodeId = LeaderOperationsHelper.getNodeId(id);

                @Override
                public Integer getNodeId() {
                    return (int) nodeId;
                }

                @Override
                public Class getNodeComponentDefinition() {
                    return HostManagerComp.class;
                }

                @Override
                public HostManagerComp.HostManagerCompInit getNodeComponentInit(VodAddress statusServer) {
                    return LeaderOperationsHelper.generateComponentInit(nodeId);
                }
            };
        }
    };




    public static Operation<NetworkOpCmd> updatePeersAddress = new Operation<NetworkOpCmd>() {

        @Override
        public NetworkOpCmd generate() {

            return new NetworkOpCmd() {

                @Override
                public DirectMsg getNetworkMsg(VodAddress origin) {
                    VodAddress destination = LeaderOperationsHelper.getUniqueAddress();
                    return new AddPeers(origin, destination, LeaderOperationsHelper.getPeersAddressCollection());
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

    
    public static Operation1<StartNodeCmd, Long> startTrueLeader = new Operation1<StartNodeCmd, Long>() {

        public StartNodeCmd generate(final Long id){

            return new StartNodeCmd<HostManagerComp>() {

                int nodeId = Integer.MIN_VALUE;
                
                @Override
                public Integer getNodeId() {
                    return nodeId;
                }

                @Override
                public Class getNodeComponentDefinition() {
                    return HostManagerComp.class;
                }

                @Override
                public HostManagerComp.HostManagerCompInit getNodeComponentInit(VodAddress statusServer) {
                    return LeaderOperationsHelper.generateComponentInit(nodeId);
                }
            };
        }
    };
    
    
    








}
