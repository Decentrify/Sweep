package se.sics.ms.search;

import se.sics.co.FailureDetectorPort;
import se.sics.gvod.config.*;
import se.sics.kompics.*;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.Timer;
import se.sics.ms.aggregator.core.StatusAggregator;
import se.sics.ms.aggregator.core.StatusAggregatorInit;
import se.sics.ms.aggregator.port.StatusAggregatorPort;
import se.sics.ms.common.ApplicationSelf;
import se.sics.ms.events.UiAddIndexEntryRequest;
import se.sics.ms.events.UiAddIndexEntryResponse;
import se.sics.ms.events.UiSearchRequest;
import se.sics.ms.events.UiSearchResponse;
import se.sics.ms.events.simEvents.AddIndexEntryP2pSimulated;
import se.sics.ms.events.simEvents.SearchP2pSimulated;
import se.sics.ms.gradient.gradient.PseudoGradient;
import se.sics.ms.gradient.gradient.PseudoGradientInit;
import se.sics.ms.gradient.gradient.SweepGradientFilter;
import se.sics.ms.gradient.misc.SimpleUtilityComparator;
import se.sics.ms.gradient.ports.GradientRoutingPort;
import se.sics.ms.gradient.ports.LeaderStatusPort;
import se.sics.ms.ports.SelfChangedPort;
import se.sics.ms.ports.SimulationEventsPort;
import se.sics.ms.ports.UiPort;
import se.sics.ms.types.SearchDescriptor;

import java.security.*;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.p2ptoolbox.chunkmanager.ChunkManagerComp;
import se.sics.p2ptoolbox.chunkmanager.ChunkManagerConfig;
import se.sics.p2ptoolbox.croupier.CroupierComp;
import se.sics.p2ptoolbox.croupier.CroupierConfig;
import se.sics.p2ptoolbox.croupier.CroupierControlPort;
import se.sics.p2ptoolbox.croupier.CroupierPort;
import se.sics.p2ptoolbox.croupier.msg.CroupierDisconnected;
import se.sics.p2ptoolbox.croupier.msg.CroupierUpdate;
import se.sics.p2ptoolbox.election.api.ports.LeaderElectionPort;
import se.sics.p2ptoolbox.election.core.ElectionConfig;
import se.sics.p2ptoolbox.election.core.ElectionFollower;
import se.sics.p2ptoolbox.election.core.ElectionInit;
import se.sics.p2ptoolbox.election.core.ElectionLeader;
import se.sics.p2ptoolbox.gradient.GradientComp;
import se.sics.p2ptoolbox.gradient.GradientConfig;
import se.sics.p2ptoolbox.gradient.GradientPort;
import se.sics.p2ptoolbox.gradient.msg.GradientUpdate;
import se.sics.p2ptoolbox.util.config.SystemConfig;

import se.sics.p2ptoolbox.util.filters.IntegerOverlayFilter;
import se.sics.p2ptoolbox.util.network.impl.BasicContentMsg;
import se.sics.p2ptoolbox.util.network.impl.DecoratedAddress;
import se.sics.p2ptoolbox.util.network.impl.DecoratedHeader;
import se.sics.util.SimpleLCPViewComparator;
import se.sics.util.SweepLeaderFilter;

public final class SearchPeer extends ComponentDefinition {

    private static final Logger log = LoggerFactory.getLogger(SearchPeer.class);

    Positive<SimulationEventsPort> indexPort = positive(SimulationEventsPort.class);
    Positive<Network> network = positive(Network.class);
    Positive<Timer> timer = positive(Timer.class);
    Negative<UiPort> internalUiPort = negative(UiPort.class);
    Positive<UiPort> externalUiPort = positive(UiPort.class);
    Positive<FailureDetectorPort> fdPort = requires(FailureDetectorPort.class);
    private Component croupier;
    private Component gradient;
    private Component search, chunkManager, aggregatorComponent, pseudoGradient;
    private Component electionLeader, electionFollower;
    private ApplicationSelf self;
    private SearchConfiguration searchConfiguration;
    private SystemConfig systemConfig;
    private GradientConfiguration pseudoGradientConfiguration;
    private ChunkManagerConfig chunkManagerConfig;

    private PublicKey publicKey;
    private PrivateKey privateKey;


    public SearchPeer(SearchPeerInit init) throws NoSuchAlgorithmException {

        // Generate the Key Pair to be used by the application.
        generateKeys();
        self = init.getSelf();

        pseudoGradientConfiguration = init.getPseudoGradientConfiguration();
        searchConfiguration = init.getSearchConfiguration();
        chunkManagerConfig = init.getChunkManagerConfig();
        systemConfig = init.getSystemConfig();

        subscribe(handleStart, control);
        subscribe(searchRequestHandler, externalUiPort);
        subscribe(addIndexEntryRequestHandler, externalUiPort);

        pseudoGradient = create(PseudoGradient.class, new PseudoGradientInit(self, pseudoGradientConfiguration));
        search = create(Search.class, new SearchInit(self, searchConfiguration, publicKey, privateKey));
        aggregatorComponent = create(StatusAggregator.class, new StatusAggregatorInit(systemConfig.aggregator, systemConfig.self , 5000));       // FIX ME: Address Set as Null.

        // External General Components in the system needs to be connected.
        connectCroupier(init.getCroupierConfiguration(), pseudoGradientConfiguration.getSeed());
        connectGradient(init.getGradientConfig(), pseudoGradientConfiguration.getSeed());
        connectElection(init.getElectionConfig(), pseudoGradientConfiguration.getSeed());
        connectChunkManager(systemConfig, chunkManagerConfig);

        // Gradient Port Connections.

        // Network Connections.
        connect(chunkManager.getPositive(Network.class), search.getNegative(Network.class));
        connect(chunkManager.getPositive(Network.class), pseudoGradient.getNegative(Network.class));
        connect(chunkManager.getPositive(Network.class), aggregatorComponent.getNegative(Network.class));

        // Timer Connections.
        connect(timer, search.getNegative(Timer.class));
        connect(timer, pseudoGradient.getNegative(Timer.class));
        connect(timer, aggregatorComponent.getNegative(Timer.class));

        // Aggregator Connections.
        connect(aggregatorComponent.getPositive(StatusAggregatorPort.class), search.getNegative(StatusAggregatorPort.class));
        connect(aggregatorComponent.getPositive(StatusAggregatorPort.class), pseudoGradient.getNegative(StatusAggregatorPort.class));

        // Internal Connections.
        connect(search.getNegative(GradientPort.class), gradient.getPositive(GradientPort.class));
        connect(pseudoGradient.getNegative(GradientPort.class), gradient.getPositive(GradientPort.class));
        connect(indexPort, search.getNegative(SimulationEventsPort.class));
        connect(search.getPositive(LeaderStatusPort.class), pseudoGradient.getNegative(LeaderStatusPort.class));
        connect(pseudoGradient.getPositive(GradientRoutingPort.class), search.getNegative(GradientRoutingPort.class));
        connect(internalUiPort, search.getPositive(UiPort.class));
        connect(search.getNegative(FailureDetectorPort.class), fdPort);
        connect(pseudoGradient.getNegative(FailureDetectorPort.class), fdPort);
        connect(search.getPositive(SelfChangedPort.class), pseudoGradient.getNegative(SelfChangedPort.class));
        subscribe(searchResponseHandler, search.getPositive(UiPort.class));
        subscribe(addIndexEntryUiResponseHandler, search.getPositive(UiPort.class));

        // ===
        // SEARCH + (PSEUDO - GRADIENT) <-- CROUPIER + GRADIENT + (LEADER - ELECTION)
        //===
        connect(croupier.getPositive(CroupierPort.class), pseudoGradient.getNegative(CroupierPort.class));
        connect(croupier.getPositive(CroupierPort.class), search.getNegative(CroupierPort.class));

        // Simulator Events.
        subscribe(addEntrySimulatorHandler, network);
        subscribe(searchSimulatorHandler, network);
    }

    /**
     * Generate the public/private key pair.
     */
    private void generateKeys() throws NoSuchAlgorithmException {

        KeyPairGenerator keyGen;
        keyGen = KeyPairGenerator.getInstance("RSA");
        keyGen.initialize(1024);
        final KeyPair key = keyGen.generateKeyPair();
        privateKey = key.getPrivate();
        publicKey = key.getPublic();
    }

    Handler<Start> handleStart = new Handler<Start>() {
        @Override
        public void handle(final Start init) {
            startCroupier();
            startGradient();
        }
    };

    /**
     * Connect the application with the leader election protocol.
     *
     * @param electionConfig Election Configuration
     * @param seed seed
     */
    private void connectElection(ElectionConfig electionConfig, int seed) {
        
        // TODO: Connection with the Status Aggregator and the Failure Detector Port remaining.
        log.info("Starting with the election components creation and connections.");

        electionLeader = create(ElectionLeader.class, new ElectionInit<ElectionLeader>(
                self.getAddress(),
                new SearchDescriptor(self.getAddress()),
                seed,
                electionConfig,
                publicKey,
                privateKey,
                new SimpleLCPViewComparator(),
                new SweepLeaderFilter()));

        electionFollower = create(ElectionFollower.class, new ElectionInit<ElectionFollower>(
                        self.getAddress(),
                        new SearchDescriptor(self.getAddress()),
                        seed,
                        electionConfig,
                        publicKey,
                        privateKey,
                        new SimpleLCPViewComparator(),
                        new SweepLeaderFilter())
                    );

        // Election leader connections.
        connect(network, electionLeader.getNegative(Network.class));
        connect(timer, electionLeader.getNegative(Timer.class));
        connect(gradient.getPositive(GradientPort.class), electionLeader.getNegative(GradientPort.class));
        connect(electionLeader.getPositive(LeaderElectionPort.class), search.getNegative(LeaderElectionPort.class));
        connect(electionLeader.getPositive(LeaderElectionPort.class), pseudoGradient.getNegative(LeaderElectionPort.class));
        
        // Election follower connections.
        connect(network, electionFollower.getNegative(Network.class));
        connect(timer, electionFollower.getNegative(Timer.class));
        connect(gradient.getPositive(GradientPort.class), electionFollower.getNegative(GradientPort.class));
        connect(electionFollower.getPositive(LeaderElectionPort.class), search.getNegative(LeaderElectionPort.class));
        connect(electionFollower.getPositive(LeaderElectionPort.class), pseudoGradient.getNegative(LeaderElectionPort.class));
    }


    /**
     * Attach the chunk manager configuration and connect it to the appropriate ports.
     * @param systemConfig system
     * @param chunkManagerConfig chunk manager config.
     */
    private void connectChunkManager(SystemConfig systemConfig, ChunkManagerConfig chunkManagerConfig) {
        
        chunkManager = create(ChunkManagerComp.class, new ChunkManagerComp.CMInit(systemConfig, chunkManagerConfig));
        connect(chunkManager.getNegative(Network.class), network);
        connect(chunkManager.getNegative(Timer.class), timer);
    }
    
    
    /**
     * Connect gradient with the application.
     * @param gradientConfig System's Gradient Configuration.
     * @param seed Seed for the Random Generator.
     */
    private void connectGradient(GradientConfig gradientConfig, int seed) {
        
        log.info("connecting gradient configuration ...");
        gradient = create(GradientComp.class, new GradientComp.GradientInit(systemConfig, gradientConfig, 0 , new SimpleUtilityComparator(), new SweepGradientFilter()));
        connect(network, gradient.getNegative(Network.class), new IntegerOverlayFilter(0));
        connect(timer, gradient.getNegative(Timer.class));
        connect(croupier.getPositive(CroupierPort.class), gradient.getNegative(CroupierPort.class));
    }

    /**
     * Boot Up the gradient service.
     */
    private void startGradient() {
        log.info("Starting Gradient component.");
        trigger(new GradientUpdate<SearchDescriptor>(new SearchDescriptor(self.getAddress())), gradient.getPositive(GradientPort.class));
    }
    
    
    private void connectCroupier(CroupierConfig config, long seed) {
        log.info("connecting croupier components...");

        List<DecoratedAddress> bootstrappingSet = new ArrayList<DecoratedAddress>();
        bootstrappingSet.addAll(systemConfig.bootstrapNodes);

        croupier = create(CroupierComp.class, new CroupierComp.CroupierInit(systemConfig, config, 0));
        connect(timer, croupier.getNegative(Timer.class));
        connect(network , croupier.getNegative(Network.class), new IntegerOverlayFilter(0));

        subscribe(handleCroupierDisconnect, croupier.getPositive(CroupierControlPort.class));
        log.debug("expecting start croupier next");
    }

    private Handler<CroupierDisconnected> handleCroupierDisconnect = new Handler<CroupierDisconnected>() {

        @Override
        public void handle(CroupierDisconnected event) {
            log.error("croupier disconnected .. ");
        }

    };

    private void startCroupier() {
        
        log.debug("Sending update to croupier.");
        trigger(new CroupierUpdate<SearchDescriptor>(new SearchDescriptor(self.getAddress())), croupier.getPositive(CroupierPort.class));
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
    

    // =====
    //  Simulator Event Handlers.
    // =====
    
    
    ClassMatchedHandler<AddIndexEntryP2pSimulated, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, AddIndexEntryP2pSimulated>> addEntrySimulatorHandler = new ClassMatchedHandler<AddIndexEntryP2pSimulated, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, AddIndexEntryP2pSimulated>>() {
        @Override
        public void handle(AddIndexEntryP2pSimulated request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, AddIndexEntryP2pSimulated> event) {
            trigger(new SimulationEventsPort.AddIndexSimulated(request.getIndexEntry()), search.getNegative(SimulationEventsPort.class));
        }
    };

    
    ClassMatchedHandler<SearchP2pSimulated, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, SearchP2pSimulated>> searchSimulatorHandler = new ClassMatchedHandler<SearchP2pSimulated, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, SearchP2pSimulated>>() {
        @Override
        public void handle(SearchP2pSimulated request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, SearchP2pSimulated> event) {
            
            log.warn("Search Event Received : {}", request.getSearchPattern());
            trigger(new SimulationEventsPort.SearchSimulated(request.getSearchPattern()), search.getNegative(SimulationEventsPort.class));
        }
    };
    
    
}
