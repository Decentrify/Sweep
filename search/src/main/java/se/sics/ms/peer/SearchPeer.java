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
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.web.Web;
import se.sics.ms.search.Search;
import se.sics.ms.search.SearchInit;
import se.sics.ms.gradient.BroadcastGradientPartnersPort;
import se.sics.ms.gradient.IndexRoutingPort;
import se.sics.ms.gradient.LeaderStatusPort;
import se.sics.ms.gradient.RoutedEventsPort;
import se.sics.ms.gradient.Gradient;
import se.sics.ms.gradient.GradientInit;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import se.sics.ms.configuration.ElectionConfiguration;
import se.sics.ms.configuration.GradientConfiguration;
import se.sics.ms.configuration.SearchConfiguration;
import se.sics.ms.election.ElectionFollower;
import se.sics.ms.election.ElectionInit;
import se.sics.ms.election.ElectionLeader;

public final class SearchPeer extends ComponentDefinition {

    public static final String CROUPIER = "CROUPIER";
    Positive<IndexPort> indexPort = positive(IndexPort.class);
    Positive<VodNetwork> network = positive(VodNetwork.class);
    Positive<Timer> timer = positive(Timer.class);
    Negative<Web> webPort = negative(Web.class);
    private Component croupier, tman, search, electionLeader, electionFollower, natTraversal;
    private Self self;
    private SearchConfiguration searchConfiguration;

    public SearchPeer() {
        natTraversal = create(NatTraverser.class);
        croupier = create(Croupier.class);
        tman = create(Gradient.class);
        search = create(Search.class);
        electionLeader = create(ElectionLeader.class);
        electionFollower = create(ElectionFollower.class);

//		connect(network, search.getNegative(VodNetwork.class));
//		connect(network, croupier.getNegative(VodNetwork.class));
//		connect(network, tman.getNegative(VodNetwork.class));
//		connect(network, electionLeader.getNegative(VodNetwork.class));
//		connect(network, electionFollower.getNegative(VodNetwork.class));
        connect(network, natTraversal.getNegative(VodNetwork.class));

        connect(natTraversal.getPositive(VodNetwork.class),
                tman.getNegative(VodNetwork.class));
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
        connect(timer, tman.getNegative(Timer.class));
        connect(timer, electionLeader.getNegative(Timer.class));
        connect(timer, electionFollower.getNegative(Timer.class));

        connect(webPort, search.getPositive(Web.class));
        connect(croupier.getPositive(PeerSamplePort.class),
                search.getNegative(PeerSamplePort.class));
        connect(croupier.getPositive(PeerSamplePort.class),
                tman.getNegative(PeerSamplePort.class));
        connect(indexPort, search.getNegative(IndexPort.class));
        connect(tman.getPositive(RoutedEventsPort.class),
                search.getNegative(RoutedEventsPort.class));
        connect(tman.getNegative(BroadcastGradientPartnersPort.class),
                electionLeader.getPositive(BroadcastGradientPartnersPort.class));
        connect(tman.getNegative(BroadcastGradientPartnersPort.class),
                electionFollower.getPositive(BroadcastGradientPartnersPort.class));
        connect(electionLeader.getNegative(LeaderStatusPort.class),
                tman.getPositive(LeaderStatusPort.class));
        connect(electionFollower.getNegative(LeaderStatusPort.class),
                tman.getPositive(LeaderStatusPort.class));
        connect(search.getNegative(IndexRoutingPort.class),
                tman.getPositive(IndexRoutingPort.class));
        connect(search.getNegative(IndexRoutingPort.class),
                electionLeader.getPositive(IndexRoutingPort.class));
        connect(electionLeader.getNegative(LeaderStatusPort.class),
                electionFollower.getPositive(LeaderStatusPort.class));

        subscribe(handleInit, control);
    }
    Handler<SearchPeerInit> handleInit = new Handler<SearchPeerInit>() {
        @Override
        public void handle(final SearchPeerInit init) {
            self = init.getSelf();
            CroupierConfiguration croupierConfiguration = init.getCroupierConfiguration();
            GradientConfiguration tmanConfiguration = init.getGradientConfiguration();
            ElectionConfiguration electionConfiguration = init.getElectionConfiguration();
            searchConfiguration = init.getApplicationConfiguration();

            trigger(new ElectionInit(self, electionConfiguration), electionLeader.getControl());
            trigger(new ElectionInit(self, electionConfiguration), electionFollower.getControl());
            trigger(new GradientInit(self, tmanConfiguration), tman.getControl());
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

            if (self.getId() == 0) {
                return;
            }

            InetAddress ip = null;
            try {
                ip = InetAddress.getLocalHost();
            } catch (UnknownHostException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
            Address peerAddress = new Address(ip, 9999, 1);
            final VodDescriptor descr = new VodDescriptor(new VodAddress(peerAddress, 1));

            LinkedList<VodDescriptor> descs = new LinkedList<VodDescriptor>();
            descs.add(0, descr);



            trigger(new CroupierJoin(descs), croupier.getPositive(CroupierPort.class));
            trigger(new SearchInit(self, searchConfiguration), search.getControl());
        }
    };
}