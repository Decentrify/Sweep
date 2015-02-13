package se.sics.ms.search;

import se.sics.cm.ChunkManager;
import se.sics.cm.ChunkManagerConfiguration;
import se.sics.cm.ChunkManagerInit;
import se.sics.cm.ports.ChunkManagerPort;
import se.sics.co.FailureDetectorPort;
import se.sics.gvod.address.Address;
import se.sics.gvod.common.Self;
import se.sics.gvod.config.*;
import se.sics.gvod.nat.traversal.NatTraverser;
import se.sics.gvod.nat.traversal.events.NatTraverserInit;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.VodNetwork;
import se.sics.gvod.timer.Timer;
import se.sics.kompics.*;
import se.sics.ms.election.ElectionFollower;
import se.sics.ms.election.ElectionInit;
import se.sics.ms.election.ElectionLeader;
import se.sics.ms.events.UiAddIndexEntryRequest;
import se.sics.ms.events.UiAddIndexEntryResponse;
import se.sics.ms.events.UiSearchRequest;
import se.sics.ms.events.UiSearchResponse;
import se.sics.ms.gradient.gradient.Gradient;
import se.sics.ms.gradient.gradient.GradientInit;
import se.sics.ms.gradient.ports.GradientRoutingPort;
import se.sics.ms.gradient.ports.GradientViewChangePort;
import se.sics.ms.gradient.ports.LeaderStatusPort;
import se.sics.ms.gradient.ports.PublicKeyPort;
import se.sics.ms.net.MessageFrameDecoder;
import se.sics.ms.ports.SelfChangedPort;
import se.sics.ms.ports.SimulationEventsPort;
import se.sics.ms.ports.UiPort;
import se.sics.ms.types.SearchDescriptor;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.p2ptoolbox.croupier.api.CroupierControlPort;
import se.sics.p2ptoolbox.croupier.api.CroupierPort;
import se.sics.p2ptoolbox.croupier.api.msg.CroupierDisconnected;
import se.sics.p2ptoolbox.croupier.api.msg.CroupierJoin;
import se.sics.p2ptoolbox.croupier.core.Croupier;
import se.sics.p2ptoolbox.croupier.core.Croupier.CroupierInit;
import se.sics.p2ptoolbox.croupier.core.CroupierConfig;

public final class SearchPeer extends ComponentDefinition {

    private static final Logger log = LoggerFactory.getLogger(SearchPeer.class);

    Positive<SimulationEventsPort> indexPort = positive(SimulationEventsPort.class);
    Positive<VodNetwork> network = positive(VodNetwork.class);
    Positive<Timer> timer = positive(Timer.class);
    Negative<UiPort> internalUiPort = negative(UiPort.class);
    Positive<UiPort> externalUiPort = positive(UiPort.class);
    Positive<FailureDetectorPort> fdPort = requires(FailureDetectorPort.class);
    private Component croupier;
    private Component gradient, search, electionLeader, electionFollower, natTraversal, chunkManager;
    private Self self;
    private SearchConfiguration searchConfiguration;

    private GradientConfiguration gradientConfiguration;
    private ElectionConfiguration electionConfiguration;
    private ChunkManagerConfiguration chunkManagerConfiguration;
    private VodAddress bootstrapingNode;

    public SearchPeer(SearchPeerInit init) {

        self = init.getSelf();
        gradientConfiguration = init.getGradientConfiguration();
        electionConfiguration = init.getElectionConfiguration();
        searchConfiguration = init.getSearchConfiguration();
        chunkManagerConfiguration = init.getChunkManagerConfiguration();
        bootstrapingNode = init.getBootstrappingNode();

        subscribe(handleStart, control);
        subscribe(searchRequestHandler, externalUiPort);
        subscribe(addIndexEntryRequestHandler, externalUiPort);

        natTraversal = create(NatTraverser.class,
                new NatTraverserInit(self, new HashSet<Address>(),
                        gradientConfiguration.getSeed(),
                        NatTraverserConfiguration.build(),
                        HpClientConfiguration.build(),
                        RendezvousServerConfiguration.build().
                        setSessionExpirationTime(30 * 1000),
                        StunServerConfiguration.build(),
                        StunClientConfiguration.build(),
                        ParentMakerConfiguration.build(), true));

        connectCroupier(init.getCroupierConfiguration(), gradientConfiguration.getSeed());
        gradient = create(Gradient.class, new GradientInit(self, gradientConfiguration));
        search = create(Search.class, new SearchInit(self, searchConfiguration));
        electionLeader = create(ElectionLeader.class,
                new ElectionInit<ElectionLeader>(self, electionConfiguration));
        electionFollower = create(ElectionFollower.class,
                new ElectionInit<ElectionFollower>(self, electionConfiguration));
        chunkManager = create(ChunkManager.class, new ChunkManagerInit<ChunkManager>(chunkManagerConfiguration,
                MessageFrameDecoder.class));

        connect(network, natTraversal.getNegative(VodNetwork.class));

        connect(natTraversal.getPositive(VodNetwork.class),
                gradient.getNegative(VodNetwork.class));
        connect(natTraversal.getPositive(VodNetwork.class),
                search.getNegative(VodNetwork.class));
        connect(natTraversal.getPositive(VodNetwork.class),
                electionLeader.getNegative(VodNetwork.class));
        connect(natTraversal.getPositive(VodNetwork.class),
                electionFollower.getNegative(VodNetwork.class));
        connect(natTraversal.getPositive(VodNetwork.class),
                chunkManager.getNegative(VodNetwork.class));

        connect(timer, natTraversal.getNegative(Timer.class));
        connect(timer, search.getNegative(Timer.class));
        connect(timer, gradient.getNegative(Timer.class));
        connect(timer, electionLeader.getNegative(Timer.class));
        connect(timer, electionFollower.getNegative(Timer.class));
        connect(timer, chunkManager.getNegative(Timer.class));

        connect(croupier.getPositive(CroupierPort.class),
                gradient.getNegative(CroupierPort.class));
        connect(indexPort, search.getNegative(SimulationEventsPort.class));

        connect(gradient.getNegative(PublicKeyPort.class),
                search.getPositive(PublicKeyPort.class));
        connect(electionLeader.getNegative(PublicKeyPort.class),
                search.getPositive(PublicKeyPort.class));
        connect(electionFollower.getNegative(PublicKeyPort.class),
                search.getPositive(PublicKeyPort.class));

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
        connect(electionFollower.getNegative(LeaderStatusPort.class),
                search.getPositive(LeaderStatusPort.class));
        connect(gradient.getPositive(GradientRoutingPort.class),
                search.getNegative(GradientRoutingPort.class));
        connect(internalUiPort, search.getPositive(UiPort.class));

        connect(search.getNegative(FailureDetectorPort.class), fdPort);
        connect(gradient.getNegative(FailureDetectorPort.class), fdPort);
        connect(electionLeader.getNegative(FailureDetectorPort.class), fdPort);
        connect(electionFollower.getNegative(FailureDetectorPort.class), fdPort);

        connect(search.getPositive(SelfChangedPort.class), gradient.getNegative(SelfChangedPort.class));

        connect(search.getNegative(ChunkManagerPort.class), chunkManager.getPositive(ChunkManagerPort.class));

        subscribe(searchResponseHandler, search.getPositive(UiPort.class));
        subscribe(addIndexEntryUiResponseHandler, search.getPositive(UiPort.class));
    }
    Handler<Start> handleStart = new Handler<Start>() {
        @Override
        public void handle(final Start init) {
            startCroupier();
        }
    };

    private void connectCroupier(CroupierConfig config, long seed) {
        log.info("connecting croupier components...");
        croupier = create(Croupier.class, new CroupierInit(config, seed, 0, self.getAddress()));
        connect(timer, croupier.getNegative(Timer.class));
        connect(natTraversal.getPositive(VodNetwork.class), croupier.getNegative(VodNetwork.class));

        subscribe(handleCroupierDisconnect, croupier.getPositive(CroupierControlPort.class));
        log.debug("expecting start croupier next");
    }

    private Handler<CroupierDisconnected> handleCroupierDisconnect = new Handler<CroupierDisconnected>() {

        @Override
        public void handle(CroupierDisconnected event) {
            log.error("croupier disconnected");
            System.exit(-1);
        }

    };

    private void startCroupier() {
        log.info("starting croupier...");
        trigger(Start.event, croupier.control());
        
        log.info("bootstrapping croupier...");
        if (bootstrapingNode != null) {
            log.error("no bootstrap node");
            System.exit(-1);
        }
        Set<VodAddress> bootstrapingSet = new HashSet<VodAddress>();
        bootstrapingSet.add(bootstrapingNode);
        trigger(new CroupierJoin(UUID.randomUUID(), bootstrapingSet), croupier.getPositive(CroupierPort.class));
        
        log.debug("expecting croupier view update next");
    }

    final Handler<UiSearchRequest> searchRequestHandler = new Handler<UiSearchRequest>() {
        @Override
        public void handle(UiSearchRequest searchRequest) {
            trigger(searchRequest, search.getPositive(UiPort.class));
        }
    };

    final Handler<UiSearchResponse> searchResponseHandler = new Handler<UiSearchResponse>() {
        @Override
        public void handle(UiSearchResponse searchResponse) {
            trigger(searchResponse, externalUiPort);
        }
    };

    final Handler<UiAddIndexEntryRequest> addIndexEntryRequestHandler = new Handler<UiAddIndexEntryRequest>() {
        @Override
        public void handle(UiAddIndexEntryRequest addIndexEntryRequest) {
            trigger(addIndexEntryRequest, search.getPositive(UiPort.class));
        }
    };

    final Handler<UiAddIndexEntryResponse> addIndexEntryUiResponseHandler = new Handler<UiAddIndexEntryResponse>() {
        @Override
        public void handle(UiAddIndexEntryResponse addIndexEntryUiResponse) {
            trigger(addIndexEntryUiResponse, externalUiPort);
        }
    };
}
