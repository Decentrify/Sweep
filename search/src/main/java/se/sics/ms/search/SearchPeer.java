package se.sics.ms.search;

import se.sics.cm.ChunkManager;
import se.sics.cm.ChunkManagerConfiguration;
import se.sics.cm.ChunkManagerInit;
import se.sics.cm.ports.ChunkManagerPort;
import se.sics.co.FailureDetectorPort;
import se.sics.gvod.address.Address;
import se.sics.gvod.common.Self;
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
import se.sics.ms.ports.SimulationEventsPort;
import se.sics.ms.ports.UiPort;
import se.sics.ms.types.SearchDescriptor;

import java.util.HashSet;
import java.util.LinkedList;

public final class SearchPeer extends ComponentDefinition {

    Positive<SimulationEventsPort> indexPort = positive(SimulationEventsPort.class);
    Positive<VodNetwork> network = positive(VodNetwork.class);
    Positive<Timer> timer = positive(Timer.class);
    Negative<UiPort> internalUiPort = negative(UiPort.class);
    Positive<UiPort> externalUiPort = positive(UiPort.class);
    Positive<FailureDetectorPort> fdPort = requires(FailureDetectorPort.class);
    private Component croupier, gradient, search, electionLeader, electionFollower, natTraversal, chunkManager;
    private Self self;
    private SearchConfiguration searchConfiguration;

    private CroupierConfiguration croupierConfiguration;
    private GradientConfiguration gradientConfiguration;
    private ElectionConfiguration electionConfiguration;
    private ChunkManagerConfiguration chunkManagerConfiguration;
    private VodAddress bootstapingNode;

    public SearchPeer(SearchPeerInit init) {

        self = init.getSelf();
        croupierConfiguration = init.getCroupierConfiguration();
        gradientConfiguration = init.getGradientConfiguration();
        electionConfiguration = init.getElectionConfiguration();
        searchConfiguration = init.getSearchConfiguration();
        chunkManagerConfiguration = init.getChunkManagerConfiguration();
        bootstapingNode = init.getBootstrappingNode();

        subscribe(handleStart, control);
        subscribe(searchRequestHandler, externalUiPort);
        subscribe(addIndexEntryRequestHandler, externalUiPort);

            natTraversal = create(NatTraverser.class,
                    new NatTraverserInit(self, new HashSet<Address>(),
                            croupierConfiguration.getSeed(),
                            NatTraverserConfiguration.build(),
                            HpClientConfiguration.build(),
                            RendezvousServerConfiguration.build().
                                    setSessionExpirationTime(30 * 1000),
                            StunServerConfiguration.build(),
                            StunClientConfiguration.build(),
                            ParentMakerConfiguration.build(), true));

            croupier = create(Croupier.class, new CroupierInit(self, croupierConfiguration));
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
                    croupier.getNegative(VodNetwork.class));
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
            connect(timer, croupier.getNegative(Timer.class));
            connect(timer, gradient.getNegative(Timer.class));
            connect(timer, electionLeader.getNegative(Timer.class));
            connect(timer, electionFollower.getNegative(Timer.class));
            connect(timer, chunkManager.getNegative(Timer.class));

            connect(croupier.getPositive(PeerSamplePort.class),
                    gradient.getNegative(PeerSamplePort.class));
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

            connect(search.getNegative(ChunkManagerPort.class),chunkManager.getPositive(ChunkManagerPort.class));

            subscribe(searchResponseHandler, search.getPositive(UiPort.class));
            subscribe(addIndexEntryUiResponseHandler, search.getPositive(UiPort.class));
    }
    Handler<Start> handleStart = new Handler<Start>() {
        @Override
        public void handle(final Start init) {

            LinkedList<SearchDescriptor> descs = new LinkedList<SearchDescriptor>();
            if (bootstapingNode != null) {
                final SearchDescriptor descr = new SearchDescriptor(bootstapingNode);
                descs.add(descr);
            }
            trigger(new CroupierJoin(SearchDescriptor.toVodDescriptorList(descs)), croupier.getPositive(CroupierPort.class));
        }
    };

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
