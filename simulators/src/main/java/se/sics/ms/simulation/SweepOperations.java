package se.sics.ms.simulation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.config.VodConfig;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.msgs.DirectMsg;
import se.sics.kompics.p2p.experiment.dsl.adaptor.Operation1;
import se.sics.ms.events.simEvents.AddIndexEntryP2pSimulated;
import se.sics.ms.main.SimulatorEncodeDecode;
import se.sics.ms.search.SearchPeer;
import se.sics.ms.search.SearchPeerInit;
import se.sics.ms.types.IndexEntry;
import se.sics.p2ptoolbox.simulator.SimulationContext;
import se.sics.p2ptoolbox.simulator.cmd.NetworkOpCmd;
import se.sics.p2ptoolbox.simulator.cmd.StartNodeCmd;

import java.io.IOException;
import java.net.UnknownHostException;

/**
 * Operations for controlling the sequence of events in sweep.
 *
 * Created by babbarshaer on 2015-02-04.
 */
public class SweepOperations {

    private static Logger logger = LoggerFactory.getLogger(SweepOperations.class);
    
    private static VodAddress nodeAddress;
    
    static{
        
        try {
            VodConfig.init(new String[0]);
            SimulatorEncodeDecode.init();
        } 
        catch (UnknownHostException e) {
            e.printStackTrace();
        } 
        catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    
    public static Operation1<StartNodeCmd, Long> startNodeCmdOperation = new Operation1<StartNodeCmd, Long>() {
            
            public StartNodeCmd generate(final Long id){
                
                return new StartNodeCmd<SearchPeer>() {
                    
                    long nodeId = SweepOperationsHelper.getStableId(id);

                    @Override
                    public Integer getNodeId() {
                        return (int) nodeId;
                    }

                    @Override
                    public Class getNodeComponentDefinition() {
                        return SearchPeer.class;
                    }

                    @Override
                    public SearchPeerInit getNodeComponentInit(VodAddress statusServer) {
                        return SweepOperationsHelper.generatePeerInit(null, nodeId);
                    }
                };
            }
    };
    
    
    public static Operation1<NetworkOpCmd,Long> addIndexEntryCommand = new Operation1<NetworkOpCmd, Long>() {
        @Override
        public NetworkOpCmd generate(final Long id) {
            
            return new NetworkOpCmd() {
                
                VodAddress address = SweepOperationsHelper.getNodeAddressToCommunicate(id);
                IndexEntry junkEntry = SweepOperationsHelper.generateIndexEntry();
                
                @Override
                public DirectMsg getNetworkMsg(VodAddress origin) {
                    
                    logger.debug("Add Index Entry id invoked for id -> " + id);
                    AddIndexEntryP2pSimulated.Request request = new AddIndexEntryP2pSimulated.Request(origin, address, junkEntry);
                    return request;
                    
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
