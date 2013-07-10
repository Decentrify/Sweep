package search.system.peer;

import java.util.LinkedList;
import java.util.Set;

import se.sics.gvod.common.Self;
import se.sics.gvod.common.VodDescriptor;
import se.sics.gvod.config.CroupierConfiguration;
import se.sics.gvod.croupier.Croupier;
import se.sics.gvod.croupier.CroupierPort;
import se.sics.gvod.croupier.PeerSamplePort;
import se.sics.gvod.croupier.events.CroupierInit;
import se.sics.gvod.croupier.events.CroupierJoin;
import se.sics.gvod.croupier.events.CroupierJoinCompleted;
import se.sics.gvod.net.VodNetwork;
import se.sics.gvod.timer.Timer;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.p2p.bootstrap.BootstrapRequest;
import se.sics.kompics.p2p.bootstrap.BootstrapResponse;
import se.sics.kompics.p2p.bootstrap.P2pBootstrap;
import se.sics.kompics.p2p.bootstrap.PeerEntry;
import se.sics.kompics.p2p.bootstrap.client.BootstrapClient;
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

public final class SearchPeer extends ComponentDefinition {
    public static final String CROUPIER = "CROUPIER";

	Positive<IndexPort> indexPort = positive(IndexPort.class);
	Positive<VodNetwork> network = positive(VodNetwork.class);
	Positive<Timer> timer = positive(Timer.class);
	Negative<Web> webPort = negative(Web.class);

	private Component croupier, tman, search, bootstrap, electionLeader, electionFollower;
    private Self self;
	private boolean bootstrapped;
	private SearchConfiguration searchConfiguration;

	public SearchPeer() {
        croupier = create(Croupier.class);
		tman = create(TMan.class);
		search = create(Search.class);
		electionLeader = create(ElectionLeader.class);
		electionFollower = create(ElectionFollower.class);
		bootstrap = create(BootstrapClient.class);

		connect(network, search.getNegative(VodNetwork.class));
		connect(network, croupier.getNegative(VodNetwork.class));
		connect(network, bootstrap.getNegative(VodNetwork.class));
		connect(network, tman.getNegative(VodNetwork.class));
		connect(network, electionLeader.getNegative(VodNetwork.class));
		connect(network, electionFollower.getNegative(VodNetwork.class));
		connect(timer, search.getNegative(Timer.class));
		connect(timer, croupier.getNegative(Timer.class));
		connect(timer, bootstrap.getNegative(Timer.class));
		connect(timer, tman.getNegative(Timer.class));
		connect(timer, electionLeader.getNegative(Timer.class));
		connect(timer, electionFollower.getNegative(Timer.class));
		connect(webPort, search.getPositive(Web.class));
		connect(search.getPositive(PeerSamplePort.class),
				croupier.getNegative(PeerSamplePort.class));
		connect(tman.getPositive(PeerSamplePort.class),
                croupier.getNegative(PeerSamplePort.class));
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
		subscribe(handleJoinCompleted, croupier.getNegative(CroupierPort.class));
		subscribe(handleBootstrapResponse, bootstrap.getPositive(P2pBootstrap.class));
	}

	Handler<SearchPeerInit> handleInit = new Handler<SearchPeerInit>() {
		@Override
		public void handle(SearchPeerInit init) {
			self = init.getSelf();
            CroupierConfiguration croupierConfiguration = init.getCyclonConfiguration();
			TManConfiguration tmanConfiguration = init.getTManConfiguration();
			ElectionConfiguration electionConfiguration = init.getElectionConfiguration();
			searchConfiguration = init.getApplicationConfiguration();
			//bootstrapRequestPeerCount = croupierConfiguration.getBootstrapRequestPeerCount();

			trigger(new ElectionInit(self, electionConfiguration), electionLeader.getControl());
			trigger(new ElectionInit(self, electionConfiguration), electionFollower.getControl());
			trigger(new TManInit(self, tmanConfiguration), tman.getControl());
            trigger(new CroupierInit(self, croupierConfiguration), croupier.getControl());
		}
	};

	Handler<BootstrapResponse> handleBootstrapResponse = new Handler<BootstrapResponse>() {
		@Override
		public void handle(BootstrapResponse event) {
			if (!bootstrapped) {
				Set<PeerEntry> somePeers = event.getPeers();
				LinkedList<VodDescriptor> insiders = new LinkedList<VodDescriptor>();

				if (somePeers == null) {
					// This should not happen but it does
					return;
				}

//				for (PeerEntry peerEntry : somePeers) {
//                    insiders.add(peerEntry.getOverlayAddress().getPeerAddress());
//				}

                trigger(new CroupierJoin(insiders), croupier.getNegative(CroupierPort.class));
				bootstrapped = true;
			}
		}
	};

	Handler<CroupierJoinCompleted> handleJoinCompleted = new Handler<CroupierJoinCompleted>() {
		@Override
		public void handle(CroupierJoinCompleted event) {
//			trigger(new BootstrapCompleted(CROUPIER, new PeerAddress(self)),
//					bootstrap.getPositive(P2pBootstrap.class));
			trigger(new SearchInit(self, searchConfiguration), search.getControl());
		}
	};
}
