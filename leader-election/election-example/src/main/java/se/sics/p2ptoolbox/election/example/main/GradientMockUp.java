package se.sics.p2ptoolbox.election.example.main;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.timer.SchedulePeriodicTimeout;
import se.sics.gvod.timer.Timeout;
import se.sics.gvod.timer.Timer;
import se.sics.kompics.*;
import se.sics.p2ptoolbox.croupier.api.util.CroupierPeerView;
import se.sics.p2ptoolbox.election.api.msg.ElectionState;
import se.sics.p2ptoolbox.election.api.msg.LeaderState;
import se.sics.p2ptoolbox.election.api.msg.LeaderUpdate;
import se.sics.p2ptoolbox.election.api.msg.ViewUpdate;
import se.sics.p2ptoolbox.election.api.msg.mock.MockedGradientUpdate;
import se.sics.p2ptoolbox.election.api.ports.LeaderElectionPort;
import se.sics.p2ptoolbox.election.api.ports.TestPort;
import se.sics.p2ptoolbox.election.example.data.PeersUpdate;
import se.sics.p2ptoolbox.election.example.ports.ApplicationPort;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Mock up of the gradient component with a separate test port.
 *
 * Created by babbar on 2015-04-01.
 */
public class GradientMockUp extends ComponentDefinition {

    private VodAddress selfAddress;
    Logger logger = LoggerFactory.getLogger(GradientMockUp.class);
    private Collection<CroupierPeerView> cpvCollection;

    // Ports
    Positive<LeaderElectionPort> leaderElectionPortPositive = requires(LeaderElectionPort.class);
    Positive<ApplicationPort> applicationPort = requires(ApplicationPort.class);
    Positive<TestPort> testPort = requires(TestPort.class);
    Positive<Timer> timerPort = requires(Timer.class);


    public GradientMockUp(GradientMockUpInit init){

        doInit(init);
        subscribe(startHandler, control);
        subscribe(peersUpdateHandler, applicationPort);
        subscribe(periodicUpdateHandler, timerPort);

        // LE Port
//        subscribe(enableLGMembershipHandler, leaderElectionPortPositive);
//        subscribe(disableLGMembershipHandler, leaderElectionPortPositive);

        subscribe(leaderElectionHandler, leaderElectionPortPositive);
        subscribe(leaderUpdateHandler, leaderElectionPortPositive);
    }

    public void doInit(GradientMockUpInit init){
        this.selfAddress = init.selfAddress;
        cpvCollection = new ArrayList<CroupierPeerView>();
    }


    /**
     * Handler for the Start event received when the component boots up.
     */
    Handler<Start> startHandler = new Handler<Start>() {
        @Override
        public void handle(Start start) {
            logger.trace("{}: Mock Component is up.", selfAddress.getId());
        }
    };

    public class PeriodicUpdate extends Timeout{
        public PeriodicUpdate(se.sics.gvod.timer.SchedulePeriodicTimeout request) {
            super(request);
        }
    }


    /**
     * Handle the event from the application regarding the peers in the system.
     */
    Handler<PeersUpdate> peersUpdateHandler = new Handler<PeersUpdate>() {
        @Override
        public void handle(PeersUpdate peersUpdate) {

            logger.debug("{}: Received event from the application.", selfAddress.getId());

            for(VodAddress address : peersUpdate.peers){
                if(address.equals(selfAddress))
                    continue;

                CroupierPeerView cpv = new CroupierPeerView(new LeaderDescriptor(address.getId(), false), address);
                cpvCollection.add(cpv);
            }

            if(cpvCollection.size() > 0){

                SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(2000, 2000);
                spt.setTimeoutEvent(new PeriodicUpdate(spt));
                trigger(spt, timerPort);

                trigger(new MockedGradientUpdate(cpvCollection), testPort);
            }
        }
    };


    /**
     * Leader selected the node as a part of its leader group.
     */
    Handler<ElectionState.EnableLGMembership> enableLGMembershipHandler = new Handler<ElectionState.EnableLGMembership>() {
        @Override
        public void handle(ElectionState.EnableLGMembership enableLGMembership) {
            logger.debug("{}: Node has now become a LG member.", selfAddress.getId());
            trigger(new ViewUpdate(null, new LeaderDescriptor(selfAddress.getId(), true)), leaderElectionPortPositive);
        }
    };


    /**
     * Node is no longer a part of the group membership.
     */
    Handler<ElectionState.EnableLGMembership> disableLGMembershipHandler = new Handler<ElectionState.EnableLGMembership>() {
        @Override
        public void handle(ElectionState.EnableLGMembership enableLGMembership) {
            logger.debug("{}: Node is no longer a LG member.", selfAddress.getId());
            trigger(new ViewUpdate(null, new LeaderDescriptor(selfAddress.getId(), false)), leaderElectionPortPositive);
        }
    };

    /**
     * Node with the highest utility in the system.
     */
    Handler<LeaderState.ElectedAsLeader> leaderElectionHandler = new Handler<LeaderState.ElectedAsLeader>() {
        @Override
        public void handle(LeaderState.ElectedAsLeader electedAsLeader) {
            logger.debug("Self is elected as the leader of the partition.");
        }
    };

    Handler<LeaderUpdate> leaderUpdateHandler = new Handler<LeaderUpdate>() {
        @Override
        public void handle(LeaderUpdate leaderUpdate) {
            logger.debug("{}: New leader has been chosen with id: {}", selfAddress.getId(),  leaderUpdate.leaderAddress.getId());
        }
    };

    /**
     * Periodically send the mocked gradient sample to the leader election component.
     */
    Handler<PeriodicUpdate> periodicUpdateHandler = new Handler<PeriodicUpdate>() {
        @Override
        public void handle(PeriodicUpdate periodicUpdate) {
            if(cpvCollection != null){
                trigger(new MockedGradientUpdate(cpvCollection) , testPort);
            }
        }
    };


    public static class GradientMockUpInit extends Init<GradientMockUp> {

        public VodAddress selfAddress;
        public GradientMockUpInit(VodAddress selfAddress){
            this.selfAddress = selfAddress;
        }

    }


}
