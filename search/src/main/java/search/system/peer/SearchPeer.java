package search.system.peer;

import common.peer.PeerDescriptor;
import se.sics.gvod.address.Address;
import se.sics.gvod.common.Self;
import se.sics.gvod.common.VodDescriptor;
import se.sics.gvod.config.CroupierConfiguration;
import se.sics.gvod.config.VodConfig;
import se.sics.gvod.croupier.Croupier;
import se.sics.gvod.croupier.CroupierPort;
import se.sics.gvod.croupier.PeerSamplePort;
import se.sics.gvod.croupier.events.CroupierInit;
import se.sics.gvod.croupier.events.CroupierJoin;
import se.sics.gvod.croupier.events.CroupierJoinCompleted;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.VodNetwork;
import se.sics.gvod.timer.Timer;
import se.sics.ipasdistances.AsIpGenerator;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.web.Web;
import search.system.peer.search.Search;
import search.system.peer.search.SearchInit;
import tman.system.peer.tman.BroadcastTManPartnersPort;
import tman.system.peer.tman.IndexRoutingPort;
import tman.system.peer.tman.LeaderStatusPort;
import tman.system.peer.tman.RoutedEventsPort;
import tman.system.peer.tman.TMan;
import tman.system.peer.tman.TManInit;

import common.configuration.ElectionConfiguration;
import common.configuration.SearchConfiguration;
import common.configuration.TManConfiguration;

import election.system.peer.election.ElectionFollower;
import election.system.peer.election.ElectionInit;
import election.system.peer.election.ElectionLeader;

import java.net.InetAddress;
import java.util.LinkedList;
import java.util.List;

public final class SearchPeer extends ComponentDefinition {
    public static final String CROUPIER = "CROUPIER";
    private AsIpGenerator ipGenerator = AsIpGenerator.getInstance(125);

	Positive<IndexPort> indexPort = positive(IndexPort.class);
	Positive<VodNetwork> network = positive(VodNetwork.class);
	Positive<Timer> timer = positive(Timer.class);
	Negative<Web> webPort = negative(Web.class);

	private Component croupier, tman, search, electionLeader, electionFollower;
    private Self self;
	private SearchConfiguration searchConfiguration;

	public SearchPeer() {
        croupier = create(Croupier.class);
		tman = create(TMan.class);
		search = create(Search.class);
		electionLeader = create(ElectionLeader.class);
		electionFollower = create(ElectionFollower.class);

		connect(network, search.getNegative(VodNetwork.class));
		connect(network, croupier.getNegative(VodNetwork.class));
		connect(network, tman.getNegative(VodNetwork.class));
		connect(network, electionLeader.getNegative(VodNetwork.class));
		connect(network, electionFollower.getNegative(VodNetwork.class));
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
		connect(tman.getNegative(BroadcastTManPartnersPort.class),
				electionLeader.getPositive(BroadcastTManPartnersPort.class));
		connect(tman.getNegative(BroadcastTManPartnersPort.class),
				electionFollower.getPositive(BroadcastTManPartnersPort.class));
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
		subscribe(handleJoinCompleted, croupier.getPositive(CroupierPort.class));
	}

	Handler<SearchPeerInit> handleInit = new Handler<SearchPeerInit>() {
		@Override
		public void handle(final SearchPeerInit init) {
			self = init.getSelf();
            CroupierConfiguration croupierConfiguration = init.getCroupierConfiguration();
			TManConfiguration tmanConfiguration = init.getTManConfiguration();
			ElectionConfiguration electionConfiguration = init.getElectionConfiguration();
			searchConfiguration = init.getApplicationConfiguration();

			trigger(new ElectionInit(self, electionConfiguration), electionLeader.getControl());
			trigger(new ElectionInit(self, electionConfiguration), electionFollower.getControl());
			trigger(new TManInit(self, tmanConfiguration), tman.getControl());
            trigger(new CroupierInit(self, croupierConfiguration), croupier.getControl());

            final VodDescriptor desc = self.getDescriptor();
            List<VodDescriptor> descriptors = new LinkedList<VodDescriptor>();
            descriptors.add(0, desc);

            if(self.getId() == 0) return;

            InetAddress ip = ipGenerator.generateIP();
            Address peerAddress = new Address(ip, VodConfig.getPort(), 0);
            final VodDescriptor descr = new VodDescriptor(new VodAddress(peerAddress, 0));

            LinkedList<VodDescriptor> descs = new LinkedList<VodDescriptor>();
            descs.add(0, descr);

            trigger(new CroupierJoin(descs), croupier.getPositive(CroupierPort.class));
		}
	};

	Handler<CroupierJoinCompleted> handleJoinCompleted = new Handler<CroupierJoinCompleted>() {
		@Override
		public void handle(CroupierJoinCompleted event) {
			trigger(new SearchInit(self, searchConfiguration), search.getControl());
		}
	};
}
