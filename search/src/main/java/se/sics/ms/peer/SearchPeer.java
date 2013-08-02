package se.sics.ms.peer;

import se.sics.gvod.address.Address;
import se.sics.gvod.common.Self;
import se.sics.gvod.common.VodDescriptor;
import se.sics.gvod.config.*;
import se.sics.gvod.croupier.Croupier;
import se.sics.gvod.croupier.CroupierPort;
import se.sics.gvod.croupier.PeerSamplePort;
import se.sics.gvod.croupier.events.CroupierInit;
import se.sics.gvod.croupier.events.CroupierJoin;
import se.sics.gvod.nat.traversal.NatTraverser;
import se.sics.gvod.nat.traversal.events.NatTraverserInit;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.VodNetwork;
import se.sics.gvod.timer.Timer;
import se.sics.kompics.*;
import se.sics.ms.election.ElectionFollower;
import se.sics.ms.election.ElectionInit;
import se.sics.ms.election.ElectionLeader;
import se.sics.ms.gradient.*;
import se.sics.ms.search.Search;
import se.sics.ms.search.SearchInit;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

public final class SearchPeer extends ComponentDefinition {

    Positive<IndexPort> indexPort = positive(IndexPort.class);
    Positive<VodNetwork> network = positive(VodNetwork.class);
    Positive<Timer> timer = positive(Timer.class);
    private Component croupier, gradient, search, electionLeader, electionFollower, natTraversal;
    private Self self;
    private SearchConfiguration searchConfiguration;
    private Random ran;

    public SearchPeer() {
        natTraversal = create(NatTraverser.class);
        croupier = create(Croupier.class);
        gradient = create(Gradient.class);
        search = create(Search.class);
        electionLeader = create(ElectionLeader.class);
        electionFollower = create(ElectionFollower.class);

        connect(network, natTraversal.getNegative(VodNetwork.class));

        connect(natTraversal.getPositive(VodNetwork.class),
                gradient.getNegative(VodNetwork.class));
        connect(natTraversal.getPositive(VodNetwork.class),
                croupier.getNegative(VodNetwork.class));
        connect(natTraversal.getPositive(VodNetwork.class),
                search.getNegative(VodNetwork.class));
        connect(natTraversal.getPositive(VodNetwork.class),
                electionLeader.getNegative(VodNetwork.class));
        connect(natTraversal.getPositive(VodNetwork.class),
                electionFollower.getNegative(VodNetwork.class));

        connect(timer, natTraversal.getNegative(Timer.class));
        connect(timer, search.getNegative(Timer.class));
        connect(timer, croupier.getNegative(Timer.class));
        connect(timer, gradient.getNegative(Timer.class));
        connect(timer, electionLeader.getNegative(Timer.class));
        connect(timer, electionFollower.getNegative(Timer.class));

        connect(croupier.getPositive(PeerSamplePort.class),
                search.getNegative(PeerSamplePort.class));
        connect(croupier.getPositive(PeerSamplePort.class),
                gradient.getNegative(PeerSamplePort.class));
        connect(indexPort, search.getNegative(IndexPort.class));
        connect(gradient.getNegative(GradientViewChangePort.class),
                electionLeader.getPositive(GradientViewChangePort.class));
        connect(gradient.getNegative(GradientViewChangePort.class),
                electionFollower.getPositive(GradientViewChangePort.class));
        connect(electionLeader.getNegative(LeaderStatusPort.class),
                gradient.getPositive(LeaderStatusPort.class));
        connect(electionLeader.getNegative(LeaderStatusPort.class),
                search.getPositive(LeaderStatusPort.class));
        connect(electionFollower.getNegative(LeaderStatusPort.class),
                gradient.getPositive(LeaderStatusPort.class));
        connect(gradient.getPositive(LeaderRequestPort.class),
                search.getNegative(LeaderRequestPort.class));

        subscribe(handleInit, control);
    }
    Handler<SearchPeerInit> handleInit = new Handler<SearchPeerInit>() {
        @Override
        public void handle(final SearchPeerInit init) {
            self = init.getSelf();
            CroupierConfiguration croupierConfiguration = init.getCroupierConfiguration();
            GradientConfiguration gradientConfiguration = init.getGradientConfiguration();
            ElectionConfiguration electionConfiguration = init.getElectionConfiguration();
            searchConfiguration = init.getSearchConfiguration();
            ran = new Random(init.getSearchConfiguration().getSeed());

            trigger(new ElectionInit(self, electionConfiguration), electionLeader.getControl());
            trigger(new ElectionInit(self, electionConfiguration), electionFollower.getControl());
            trigger(new GradientInit(self, gradientConfiguration), gradient.getControl());
            trigger(new CroupierInit(self, croupierConfiguration), croupier.getControl());
            trigger(new NatTraverserInit(self, new HashSet<Address>(), croupierConfiguration.getSeed(), NatTraverserConfiguration.build(),
                    HpClientConfiguration.build(),
                    RendezvousServerConfiguration.build().
                    setSessionExpirationTime(30 * 1000),
                    StunClientConfiguration.build(),
                    StunServerConfiguration.build(),
                    ParentMakerConfiguration.build(), true), natTraversal.control());

            final VodDescriptor desc = self.getDescriptor();
            List<VodDescriptor> descriptors = new LinkedList<VodDescriptor>();
            descriptors.add(0, desc);

            InetAddress ip = null;
            try {
                ip = InetAddress.getLocalHost();
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }

            LinkedList<VodDescriptor> descs = new LinkedList<VodDescriptor>();
            if (self.getId() > 0) {
                Address peerAddress = new Address(ip, 9999, ran.nextInt(self.getId()));
                final VodDescriptor descr = new VodDescriptor(new VodAddress(peerAddress, 1));
                descs.add(0, descr);
            }

            trigger(new CroupierJoin(descs), croupier.getPositive(CroupierPort.class));
            trigger(new SearchInit(self, searchConfiguration), search.getControl());
        }
    };
}
