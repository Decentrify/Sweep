package se.sics.p2ptoolbox.election.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.VodNetwork;
import se.sics.gvod.timer.Timer;
import se.sics.kompics.*;
import se.sics.p2ptoolbox.croupier.api.util.PeerView;
import se.sics.p2ptoolbox.election.api.ports.LeaderElectionPort;

/**
 * Leader Election Component.
 *  
 * This is the core component which is responsible for election and the maintainence of the leader in the system.
 * The nodes are constantly analyzing the samples from the sampling service and based on the convergence tries to assert themselves
 * as the leader.
 * 
 * <br/><br/>
 * 
 * In addition to this, this component works on leases in which the leader generates a lease 
 * and adds could happen only for that lease. The leader,  when the lease is on the verge of expiring tries to renew the lease by sending a special message to the
 * nodes in its view. 
 *   
 * <br/><br/>
 * 
 * <b>NOTE: </b> The lease needs to be short enough so that if the leader dies, the system is not in a transitive state.
 *
 * <b>CAUTION: </b> Under development, so unstable to use as it is.
 * Created by babbarshaer on 2015-03-27.
 */
public class LeaderElection extends ComponentDefinition{

    Logger logger = LoggerFactory.getLogger(LeaderElection.class);
    
    private PeerView selfPV;
    private ElectionConfig config;
    private long seed;
    private VodAddress selfAddress;
    
    // Ports.
    Positive<Timer> timerPositive = requires(Timer.class);
    Positive<VodNetwork> networkPositive = requires(VodNetwork.class);
    Negative<LeaderElectionPort> electionPort = provides(LeaderElectionPort.class);
    
    public LeaderElection(LeaderElectionInit init){
        doInit(init);
        subscribe(startHandler, control);
    }

    // Init method.
    private void doInit(LeaderElectionInit init) {
        this.selfPV = init.selfView;
        this.config = init.electionConfig;
        this.seed = init.seed;
        this.selfAddress = init.selfAddress;
    }

    Handler<Start> startHandler = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            logger.debug("Started the Leader Election Component ...");
        }
    };
    
    
    
    
    
    
    
    
    /**
     * Initialization Class for the Leader Election Component.
     */
    public static class LeaderElectionInit extends Init<LeaderElection>{
        
        VodAddress selfAddress;
        PeerView selfView;
        long seed;
        ElectionConfig electionConfig;
        
        public LeaderElectionInit(VodAddress selfAddress, PeerView selfView, long seed, ElectionConfig electionConfig){
            this.selfAddress = selfAddress;
            this.selfView = selfView;
            this.seed = seed;
            this.electionConfig = electionConfig;
        }
    }
    
}
