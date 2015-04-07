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
import se.sics.ms.aggregator.core.StatusAggregator;
import se.sics.ms.aggregator.core.StatusAggregatorInit;
import se.sics.ms.aggregator.port.StatusAggregatorPort;
import se.sics.ms.events.UiAddIndexEntryRequest;
import se.sics.ms.events.UiAddIndexEntryResponse;
import se.sics.ms.events.UiSearchRequest;
import se.sics.ms.events.UiSearchResponse;
import se.sics.ms.events.simEvents.AddIndexEntryP2pSimulated;
import se.sics.ms.gradient.gradient.PseudoGradient;
import se.sics.ms.gradient.gradient.PseudoGradientInit;
import se.sics.ms.gradient.gradient.SweepGradientFilter;
import se.sics.ms.gradient.misc.SimpleUtilityComparator;
import se.sics.ms.gradient.ports.GradientRoutingPort;
import se.sics.ms.gradient.ports.GradientViewChangePort;
import se.sics.ms.gradient.ports.LeaderStatusPort;
import se.sics.ms.gradient.ports.PublicKeyPort;
import se.sics.ms.net.MessageFrameDecoder;
import se.sics.ms.ports.SelfChangedPort;
import se.sics.ms.ports.SimulationEventsPort;
import se.sics.ms.ports.UiPort;
import se.sics.ms.types.SearchDescriptor;

import java.security.*;
import java.util.HashSet;
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
import se.sics.p2ptoolbox.election.api.ports.LeaderElectionPort;
import se.sics.p2ptoolbox.election.core.ElectionConfig;
import se.sics.p2ptoolbox.election.core.ElectionFollower;
import se.sics.p2ptoolbox.election.core.ElectionInit;
import se.sics.p2ptoolbox.election.core.ElectionLeader;
import se.sics.p2ptoolbox.gradient.api.GradientPort;
import se.sics.p2ptoolbox.gradient.api.msg.GradientUpdate;
import se.sics.p2ptoolbox.gradient.core.Gradient;
import se.sics.p2ptoolbox.gradient.core.GradientConfig;
import se.sics.p2ptoolbox.serialization.filter.OverlayHeaderFilter;
import se.sics.util.SimpleLCPViewComparator;
import se.sics.util.SweepLeaderFilter;

public final class SearchPeer extends ComponentDefinition {

    private static final Logger log = LoggerFactory.getLogger(SearchPeer.class);

    Positive<SimulationEventsPort> indexPort = positive(SimulationEventsPort.class);
    Positive<VodNetwork> network = positive(VodNetwork.class);
    Positive<Timer> timer = positive(Timer.class);
    Negative<UiPort> internalUiPort = negative(UiPort.class);
    Positive<UiPort> externalUiPort = positive(UiPort.class);
    Positive<FailureDetectorPort> fdPort = requires(FailureDetectorPort.class);
    private Component croupier;
    private Component gradient;
//    private Component search, electionLeader, electionFollower, natTraversal, chunkManager, aggregatorComponent, pseudoGradient;
    private Component search, natTraversal, chunkManager, aggregatorComponent, pseudoGradient;
    private Component electionLeader, electionFollower;
    private Self self;
    private VodAddress simulatorAddress;
    private SearchConfiguration searchConfiguration;

    private GradientConfiguration pseudoGradientConfiguration;
    private ElectionConfiguration electionConfiguration;
    private ChunkManagerConfiguration chunkManagerConfiguration;
    private VodAddress bootstrapingNode;

    private PublicKey publicKey;
    private PrivateKey privateKey;


    public SearchPeer(SearchPeerInit init) throws NoSuchAlgorithmException {

        // Generate the Key Pair to be used by the application.
        generateKeys();
        self = init.getSelf();
        
        simulatorAddress = init.getSimulatorAddress();
        pseudoGradientConfiguration = init.getPseudoGradientConfiguration();
        electionConfiguration = init.getElectionConfiguration();
        searchConfiguration = init.getSearchConfiguration();
        chunkManagerConfiguration = init.getChunkManagerConfiguration();
        bootstrapingNode = init.getBootstrappingNode();

        subscribe(handleStart, control);
        subscribe(searchRequestHandler, externalUiPort);
        subscribe(addIndexEntryRequestHandler, externalUiPort);

        natTraversal = create(NatTraverser.class,
                new NatTraverserInit(self, new HashSet<Address>(),
                        pseudoGradientConfiguration.getSeed(),
                        NatTraverserConfiguration.build(),
                        HpClientConfiguration.build(),
                        RendezvousServerConfiguration.build().
                        setSessionExpirationTime(30 * 1000),
                        StunServerConfiguration.build(),
                        StunClientConfiguration.build(),
                        ParentMakerConfiguration.build(), true));


        

        pseudoGradient = create(PseudoGradient.class, new PseudoGradientInit(self, pseudoGradientConfiguration));
        search = create(Search.class, new SearchInit(self, searchConfiguration, publicKey, privateKey));
        chunkManager = create(ChunkManager.class, new ChunkManagerInit<ChunkManager>(chunkManagerConfiguration,
                MessageFrameDecoder.class));
        aggregatorComponent = create(StatusAggregator.class, new StatusAggregatorInit(simulatorAddress, self.getAddress(), 5000));

        // External General Components in the system needs to be connected.
        connectCroupier(init.getCroupierConfiguration(), pseudoGradientConfiguration.getSeed());
        connectGradient(init.getGradientConfig(), pseudoGradientConfiguration.getSeed());
        connectElection(init.getElectionConfig(), pseudoGradientConfiguration.getSeed());

        connect(network, natTraversal.getNegative(VodNetwork.class));
        // Gradient Port Connections.
        connect(search.getNegative(GradientPort.class), gradient.getPositive(GradientPort.class));
        connect(pseudoGradient.getNegative(GradientPort.class), gradient.getPositive(GradientPort.class));

        connect(natTraversal.getPositive(VodNetwork.class),
                pseudoGradient.getNegative(VodNetwork.class));
        connect(natTraversal.getPositive(VodNetwork.class),
                search.getNegative(VodNetwork.class));
        
        connect(natTraversal.getPositive(VodNetwork.class),
                chunkManager.getNegative(VodNetwork.class));
        connect(natTraversal.getPositive(VodNetwork.class), 
                aggregatorComponent.getNegative(VodNetwork.class));
        
        // Other Components and Aggregator Component.
        connect(aggregatorComponent.getPositive(StatusAggregatorPort.class), 
                search.getNegative(StatusAggregatorPort.class));
        connect(aggregatorComponent.getPositive(StatusAggregatorPort.class),
                pseudoGradient.getNegative(StatusAggregatorPort.class));

        connect(timer, natTraversal.getNegative(Timer.class));
        connect(timer, search.getNegative(Timer.class));
        
        connect(timer, pseudoGradient.getNegative(Timer.class));
        connect(timer, chunkManager.getNegative(Timer.class));
        connect(timer, aggregatorComponent.getNegative(Timer.class));
        
        // ===
        // SEARCH + (PSEUDO - GRADIENT) <-- CROUPIER + GRADIENT + (LEADER - ELECTION)
        //===
        connect(croupier.getPositive(CroupierPort.class), 
                pseudoGradient.getNegative(CroupierPort.class));
        connect(croupier.getPositive(CroupierPort.class),
                search.getNegative(CroupierPort.class));
        
        connect(indexPort, search.getNegative(SimulationEventsPort.class));

//        connect(pseudoGradient.getNegative(PublicKeyPort.class),
//                search.getPositive(PublicKeyPort.class));
        
        connect(pseudoGradient.getPositive(GradientRoutingPort.class),
                search.getNegative(GradientRoutingPort.class));
        connect(internalUiPort, search.getPositive(UiPort.class));
        
        connect(search.getNegative(FailureDetectorPort.class), fdPort);
        connect(pseudoGradient.getNegative(FailureDetectorPort.class), fdPort);
        connect(search.getPositive(SelfChangedPort.class), pseudoGradient.getNegative(SelfChangedPort.class));

        connect(search.getNegative(ChunkManagerPort.class), chunkManager.getPositive(ChunkManagerPort.class));
        subscribe(searchResponseHandler, search.getPositive(UiPort.class));
        subscribe(addIndexEntryUiResponseHandler, search.getPositive(UiPort.class));


        // Simulator Events.
        subscribe(addEntrySimulatorEventHandler, network);
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
        connect(natTraversal.getPositive(VodNetwork.class), electionLeader.getNegative(VodNetwork.class));
        connect(timer, electionLeader.getNegative(Timer.class));
        connect(gradient.getPositive(GradientPort.class), electionLeader.getNegative(GradientPort.class));
        connect(electionLeader.getPositive(LeaderElectionPort.class), search.getNegative(LeaderElectionPort.class));
        connect(electionLeader.getPositive(LeaderElectionPort.class), pseudoGradient.getNegative(LeaderElectionPort.class));
        
        // Election follower connections.
        connect(natTraversal.getPositive(VodNetwork.class), electionFollower.getNegative(VodNetwork.class));
        connect(timer, electionFollower.getNegative(Timer.class));
        connect(gradient.getPositive(GradientPort.class), electionFollower.getNegative(GradientPort.class));
        connect(electionFollower.getPositive(LeaderElectionPort.class), search.getNegative(LeaderElectionPort.class));
        connect(electionFollower.getPositive(LeaderElectionPort.class), pseudoGradient.getNegative(LeaderElectionPort.class));
    }


    /**
     * Connect gradient with the application.
     * @param gradientConfig System's Gradient Configuration.
     * @param seed Seed for the Random Generator.
     */
    private void connectGradient(GradientConfig gradientConfig, int seed) {
        
        log.info("connecting gradient configuration ...");
        gradient = create(Gradient.class, new Gradient.GradientInit(self.getAddress(),gradientConfig, 1, new SimpleUtilityComparator(), new SweepGradientFilter(), seed));
        connect(natTraversal.getPositive(VodNetwork.class), gradient.getNegative(VodNetwork.class));
        connect(timer, gradient.getNegative(Timer.class));
        connect(croupier.getPositive(CroupierPort.class), gradient.getNegative(CroupierPort.class));    
    }

    /**
     * Boot Up the gradient service.
     */
    private void startGradient() {
        log.info("Starting Gradient component.");
        trigger(new GradientUpdate(new SearchDescriptor(self.getAddress())), gradient.getPositive(GradientPort.class));
    }
    
    
    private void connectCroupier(CroupierConfig config, long seed) {
        log.info("connecting croupier components...");
        croupier = create(Croupier.class, new CroupierInit(config, seed, 0, self.getAddress()));
        connect(timer, croupier.getNegative(Timer.class));
        connect(natTraversal.getPositive(VodNetwork.class), croupier.getNegative(VodNetwork.class), new OverlayHeaderFilter(0));

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
        
        log.info("bootstrapping croupier...");
        Set<VodAddress> bootstrappingSet = new HashSet<VodAddress>();
        
        // Update Set if bootstrap node is not null.
        if (bootstrapingNode != null && !self.getAddress().equals(bootstrapingNode)) {
            bootstrappingSet.add(bootstrapingNode);
        }
        
        trigger(new CroupierJoin(UUID.randomUUID(), bootstrappingSet), croupier.getPositive(CroupierControlPort.class));
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
    
    
    // ===== 
    //  Simulator Event Handlers.
    // =====
    
    Handler<AddIndexEntryP2pSimulated.Request> addEntrySimulatorEventHandler = new Handler<AddIndexEntryP2pSimulated.Request>() {
        
        @Override
        public void handle(AddIndexEntryP2pSimulated.Request event) {
            
            log.debug("{}: Received PeerJoin Simulated Event. ", self.getId());
            trigger(new SimulationEventsPort.AddIndexSimulated(event.getIndexEntry()), search.getNegative(SimulationEventsPort.class));
        }
    };
    
}
