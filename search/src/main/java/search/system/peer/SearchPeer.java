package search.system.peer;

import java.util.LinkedList;
import java.util.Set;

import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Network;
import se.sics.kompics.p2p.bootstrap.BootstrapCompleted;
import se.sics.kompics.p2p.bootstrap.BootstrapRequest;
import se.sics.kompics.p2p.bootstrap.BootstrapResponse;
import se.sics.kompics.p2p.bootstrap.P2pBootstrap;
import se.sics.kompics.p2p.bootstrap.PeerEntry;
import se.sics.kompics.p2p.bootstrap.client.BootstrapClient;
import se.sics.kompics.p2p.bootstrap.client.BootstrapClientInit;
import se.sics.kompics.timer.Timer;
import se.sics.kompics.web.Web;
import search.system.peer.search.Search;
import search.system.peer.search.SearchInit;
import tman.system.peer.tman.BroadcastTManPartnersPort;
import tman.system.peer.tman.IndexRoutingPort;
import tman.system.peer.tman.LeaderStatusPort;
import tman.system.peer.tman.RoutedEventsPort;
import tman.system.peer.tman.TMan;
import tman.system.peer.tman.TManInit;

import common.configuration.CyclonConfiguration;
import common.configuration.ElectionConfiguration;
import common.configuration.SearchConfiguration;
import common.configuration.TManConfiguration;
import common.peer.PeerAddress;

import cyclon.Cyclon;
import cyclon.CyclonInit;
import cyclon.CyclonJoin;
import cyclon.CyclonPort;
import cyclon.CyclonSamplePort;
import cyclon.JoinCompleted;
import election.system.peer.election.ElectionFollower;
import election.system.peer.election.ElectionInit;
import election.system.peer.election.ElectionLeader;

public final class SearchPeer extends ComponentDefinition {
	public static final String CYCLON = "Cyclon";

	Positive<IndexPort> indexPort = positive(IndexPort.class);
	Positive<Network> network = positive(Network.class);
	Positive<Timer> timer = positive(Timer.class);
	Negative<Web> webPort = negative(Web.class);

	private Component cyclon, tman, search, bootstrap, electionLeader, electionFollower;
	private Address self;
	private int bootstrapRequestPeerCount;
	private boolean bootstrapped;
	private SearchConfiguration searchConfiguration;

	public SearchPeer() {
		cyclon = create(Cyclon.class);
		tman = create(TMan.class);
		search = create(Search.class);
		electionLeader = create(ElectionLeader.class);
		electionFollower = create(ElectionFollower.class);
		bootstrap = create(BootstrapClient.class);

		connect(network, search.getNegative(Network.class));
		connect(network, cyclon.getNegative(Network.class));
		connect(network, bootstrap.getNegative(Network.class));
		connect(network, tman.getNegative(Network.class));
		connect(network, electionLeader.getNegative(Network.class));
		connect(network, electionFollower.getNegative(Network.class));
		connect(timer, search.getNegative(Timer.class));
		connect(timer, cyclon.getNegative(Timer.class));
		connect(timer, bootstrap.getNegative(Timer.class));
		connect(timer, tman.getNegative(Timer.class));
		connect(timer, electionLeader.getNegative(Timer.class));
		connect(timer, electionFollower.getNegative(Timer.class));
		connect(webPort, search.getPositive(Web.class));
		connect(cyclon.getPositive(CyclonSamplePort.class),
				search.getNegative(CyclonSamplePort.class));
		connect(cyclon.getPositive(CyclonSamplePort.class),
				tman.getNegative(CyclonSamplePort.class));
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
		subscribe(handleJoinCompleted, cyclon.getPositive(CyclonPort.class));
		subscribe(handleBootstrapResponse, bootstrap.getPositive(P2pBootstrap.class));
	}

	Handler<SearchPeerInit> handleInit = new Handler<SearchPeerInit>() {
		@Override
		public void handle(SearchPeerInit init) {
			self = init.getPeerSelf();
			CyclonConfiguration cyclonConfiguration = init.getCyclonConfiguration();
			TManConfiguration tmanConfiguration = init.getTManConfiguration();
			ElectionConfiguration electionConfiguration = init.getElectionConfiguration();
			searchConfiguration = init.getApplicationConfiguration();
			bootstrapRequestPeerCount = cyclonConfiguration.getBootstrapRequestPeerCount();

			trigger(new ElectionInit(self, electionConfiguration), electionLeader.getControl());
			trigger(new ElectionInit(self, electionConfiguration), electionFollower.getControl());
			trigger(new TManInit(self, tmanConfiguration), tman.getControl());
			trigger(new CyclonInit(self, cyclonConfiguration), cyclon.getControl());
			trigger(new BootstrapClientInit(self, init.getBootstrapConfiguration()),
					bootstrap.getControl());
			BootstrapRequest request = new BootstrapRequest(CYCLON, bootstrapRequestPeerCount);
			trigger(request, bootstrap.getPositive(P2pBootstrap.class));
		}
	};

	Handler<BootstrapResponse> handleBootstrapResponse = new Handler<BootstrapResponse>() {
		@Override
		public void handle(BootstrapResponse event) {
			if (!bootstrapped) {
				Set<PeerEntry> somePeers = event.getPeers();
				LinkedList<Address> cyclonInsiders = new LinkedList<Address>();

				if (somePeers == null) {
					// This should not happen but it does
					return;
				}

				for (PeerEntry peerEntry : somePeers) {
					cyclonInsiders.add(peerEntry.getOverlayAddress().getPeerAddress());
				}

				trigger(new CyclonJoin(self, cyclonInsiders), cyclon.getPositive(CyclonPort.class));
				bootstrapped = true;
			}
		}
	};

	Handler<JoinCompleted> handleJoinCompleted = new Handler<JoinCompleted>() {
		@Override
		public void handle(JoinCompleted event) {
			trigger(new BootstrapCompleted(CYCLON, new PeerAddress(self)),
					bootstrap.getPositive(P2pBootstrap.class));
			trigger(new SearchInit(self, searchConfiguration), search.getControl());
		}
	};
}
