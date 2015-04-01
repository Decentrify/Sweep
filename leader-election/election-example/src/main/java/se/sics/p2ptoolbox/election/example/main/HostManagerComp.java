package se.sics.p2ptoolbox.election.example.main;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.VodNetwork;
import se.sics.gvod.timer.Timer;
import se.sics.kompics.*;
import se.sics.p2ptoolbox.election.api.LCPeerView;
import se.sics.p2ptoolbox.election.api.ports.LeaderElectionPort;
import se.sics.p2ptoolbox.election.core.ElectionLeader;
import se.sics.p2ptoolbox.election.core.ElectionConfig;
import se.sics.p2ptoolbox.election.core.ElectionFollower;
import se.sics.p2ptoolbox.election.example.data.PeersUpdate;
import se.sics.p2ptoolbox.election.example.mockup.ports.ApplicationPort;
import se.sics.p2ptoolbox.election.example.msg.AddPeers;

import java.util.Comparator;

/**
 * Main component the would encapsulate the other components.
 *
 * Created by babbar on 2015-04-01.
 */
public class HostManagerComp extends ComponentDefinition{

    Positive<VodNetwork> networkPositive = requires(VodNetwork.class);
    Positive<Timer> timerPositive = requires(Timer.class);
    Negative<ApplicationPort> applicationPort = provides(ApplicationPort.class);

    Logger logger = LoggerFactory.getLogger(HostManagerComp.class);

    private VodAddress selfAddress;
    private long leaseTimeout;
    private LeaderDescriptor selfView;

    Component electionLeader, electionFollower;
    Component gradientMockUp;


    public HostManagerComp(HostManagerCompInit init){

        doInit(init);

        // Create Configuration for election components.
        ElectionConfig.ElectionConfigBuilder builder = new ElectionConfig.ElectionConfigBuilder(init.lcpComparator, init.viewSize, null, null);
        builder.setLeaseTime(30000)
                .setConvergenceRounds(4)
                .setConvergenceTest(0.9d);

        ElectionConfig config = builder.buildElectionConfig();

        // Create necessary components.
        electionLeader = create(ElectionLeader.class, new ElectionLeader.LeaderElectionInit(selfAddress, selfView, 100, config));
        electionFollower = create(ElectionFollower.class, new ElectionFollower.ElectionFollowerInit(selfAddress, selfView, 100, config));
        gradientMockUp = create(GradientMockUp.class, new GradientMockUp.GradientMockUpInit(selfAddress));

        // Make the necessary connections.
        connect(electionLeader.getNegative(VodNetwork.class), networkPositive);
        connect(electionLeader.getNegative(Timer.class), timerPositive);

        connect(electionFollower.getNegative(VodNetwork.class), networkPositive);
        connect(electionFollower.getNegative(Timer.class), timerPositive);

        // Connections with the mock up component.
        connect(electionLeader.getPositive(LeaderElectionPort.class), gradientMockUp.getNegative(LeaderElectionPort.class));
        connect(electionFollower.getPositive(LeaderElectionPort.class), gradientMockUp.getNegative(LeaderElectionPort.class));

        connect(gradientMockUp.getNegative(Timer.class), timerPositive);
        connect(applicationPort, gradientMockUp.getPositive(ApplicationPort.class));

        subscribe(startHandler, control);
    }

    private void doInit(HostManagerCompInit init) {

        selfAddress = init.selfAddress;
        leaseTimeout = init.leaseTimeout;
        selfView = new LeaderDescriptor(selfAddress.getId());

        // TODO: Add Common Encode Decode for the Leader Election Mechanism.
    }


    Handler<Start> startHandler = new Handler<Start>(){
        @Override
        public void handle(Start start) {
            logger.debug(" {}: Host Manager Component Started .... ", selfAddress.getId());
        }
    };



    Handler<AddPeers> addPeershandler = new Handler<AddPeers>() {
        @Override
        public void handle(AddPeers addPeers) {

            trigger(new PeersUpdate(addPeers.peers), applicationPort);
        }
    };


    /**
     * Init class for the main component.
     */
    public static class HostManagerCompInit extends Init<HostManagerComp>{

        VodAddress selfAddress;
        long leaseTimeout;
        Comparator<LCPeerView> lcpComparator;
        private int viewSize;

        public HostManagerCompInit(VodAddress selfAddress, long leaseTimeout, Comparator<LCPeerView> lcpComparator, int viewSize){

            this.selfAddress = selfAddress;
            this.leaseTimeout = leaseTimeout;
            this.lcpComparator = lcpComparator;
            this.viewSize = viewSize;

        }
    }




}
