package se.sics.ms.search;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.co.FailureDetectorPort;
import se.sics.gvod.config.*;
import se.sics.kompics.*;
import se.sics.kompics.network.Network;
import se.sics.kompics.network.Transport;
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
import se.sics.ms.gradient.events.PAGUpdate;
import se.sics.ms.gradient.gradient.*;
import se.sics.ms.gradient.misc.SimpleUtilityComparator;
import se.sics.ms.gradient.ports.GradientRoutingPort;
import se.sics.ms.gradient.ports.LeaderStatusPort;
import se.sics.ms.gradient.ports.PAGPort;
import se.sics.ms.ports.SelfChangedPort;
import se.sics.ms.ports.SimulationEventsPort;
import se.sics.ms.ports.UiPort;
import se.sics.ms.types.SearchDescriptor;

import java.security.*;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.ms.util.CommonHelper;
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
import se.sics.p2ptoolbox.gradient.temp.UpdatePort;
import se.sics.p2ptoolbox.tgradient.TreeGradientComp;
import se.sics.p2ptoolbox.tgradient.TreeGradientConfig;
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
    private Component gradient, tgradient;
    private Component search, chunkManager, aggregatorComponent, pseudoGradient;
    private Component partitionAwareGradient;
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
        search = create(NPAwareSearch.class, new SearchInit(self, searchConfiguration, publicKey, privateKey));
        aggregatorComponent = create(StatusAggregator.class, new StatusAggregatorInit(systemConfig.aggregator, systemConfig.self , 5000));       // FIX ME: Address Set as Null.

        // External Components creating and connection to the local components.
        connectCroupier(init.getCroupierConfiguration());
//		  connectPAG(systemConfig, init.getGradientConfig());     // connect pag with system.
//        connectTreeGradient(init.getTGradientConfig(),  init.getGradientConfig());
        connectGradient(init.getGradientConfig(), pseudoGradientConfiguration.getSeed());
        connectElection(init.getElectionConfig(), pseudoGradientConfiguration.getSeed());
        connectChunkManager(systemConfig, chunkManagerConfig);
        
        // Internal Component Connections.
        doInternalConnections();
        
        // Subscriptions.
        subscribe(searchResponseHandler, search.getPositive(UiPort.class));
        subscribe(addIndexEntryUiResponseHandler, search.getPositive(UiPort.class));
        subscribe(addEntrySimulatorHandler, network);
        subscribe(searchSimulatorHandler, network);
        subscribe(handleStart, control);
        subscribe(searchRequestHandler, externalUiPort);
        subscribe(addIndexEntryRequestHandler, externalUiPort);
        
    }

        // Gradient Port Connections.

    /**
     * Perform the internal connections among the components
     * that are local to the application. In other words, connect the components
     */
    private void doInternalConnections(){
        
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
        connect(search.getPositive(SelfChangedPort.class), pseudoGradient.getNegative(SelfChangedPort.class));
        
    }
    
    
    
    /**
     * Connect the application with the partition aware 
     * gradient. The PAG will enclose the gradient, so connection to the 
     * PAG will be similar to the gradient.
     *  
     * @param systemConfig system configuration.
     * @param gradientConfig gradient configuration.
     */
    private void connectPAG(SystemConfig systemConfig, GradientConfig gradientConfig) {
        
        log.debug("Initiating the connection to the partition aware gradient.");
        
        PAGInit init = new PAGInit(systemConfig, gradientConfig, self.getAddress().getBase(), 0, 50);
        partitionAwareGradient = create(PartitionAwareGradient.class, init);
        
        connect(partitionAwareGradient.getNegative(Timer.class), timer);
        connect(network, partitionAwareGradient.getNegative(Network.class), new IntegerOverlayFilter(0));
        connect(croupier.getPositive(CroupierPort.class), partitionAwareGradient.getNegative(CroupierPort.class));
        connect(partitionAwareGradient.getPositive(PAGPort.class), search.getNegative(PAGPort.class));
        connect(partitionAwareGradient.getPositive(GradientPort.class), search.getNegative(GradientPort.class));
        connect(partitionAwareGradient.getPositive(GradientPort.class), pseudoGradient.getNegative(GradientPort.class));
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
     * Initialize the PAG service.
     */
    private void startPAG(){
        log.debug("Sending initial self update to the PAG ... ");
        trigger(new PAGUpdate(new SearchDescriptor(self.getAddress())), partitionAwareGradient.getPositive(PAGPort.class));
        trigger(new GradientUpdate<SearchDescriptor>(new SearchDescriptor(self.getAddress())), partitionAwareGradient.getPositive(GradientPort.class));
    }


    /**
     * Connect gradient with the application.
     * @param gradientConfig System's Gradient Configuration.
     * @param seed Seed for the Random Generator.
     */
    private void connectGradient(GradientConfig gradientConfig, int seed) {
        
        log.info("connecting gradient configuration ...");
        gradient = create(GradientComp.class, new GradientComp.GradientInit(systemConfig, gradientConfig, 1 , new SimpleUtilityComparator(), new SweepGradientFilter()));
        connect(network, gradient.getNegative(Network.class), new IntegerOverlayFilter(1));
        connect(timer, gradient.getNegative(Timer.class));
        connect(croupier.getPositive(CroupierPort.class), gradient.getNegative(CroupierPort.class));
    }

    /**
     * Connect gradient with the application.
     * @param gradientConfig System's Gradient Configuration.
     */
    private void connectTreeGradient(TreeGradientConfig tgradientConfig, GradientConfig gradientConfig) {

        log.info("connecting tree gradient configuration ...");
        tgradient = create(TreeGradientComp.class, new TreeGradientComp.TreeGradientInit(systemConfig, gradientConfig, tgradientConfig, 2 , new SweepGradientFilter()));
        connect(network, tgradient.getNegative(Network.class), new IntegerOverlayFilter(2));
        connect(timer, tgradient.getNegative(Timer.class));
        connect(croupier.getPositive(CroupierPort.class), tgradient.getNegative(CroupierPort.class));
        connect(gradient.getPositive(GradientPort.class), tgradient.getNegative(GradientPort.class));
        connect(gradient.getPositive(UpdatePort.class), tgradient.getNegative(UpdatePort.class));

    }

    /**
     * Boot Up the gradient service.
     */
    private void startGradient() {
        log.info("Starting Gradient component.");
        trigger(new GradientUpdate<SearchDescriptor>(new SearchDescriptor(self.getAddress())), gradient.getPositive(GradientPort.class));
    }
    
    
    private void connectCroupier( CroupierConfig config ) {
        log.info("connecting croupier components...");

        List<DecoratedAddress> bootstrappingSet = new ArrayList<DecoratedAddress>();
        bootstrappingSet.addAll(systemConfig.bootstrapNodes);

        croupier = create(CroupierComp.class, new CroupierComp.CroupierInit(systemConfig, config, 0));
        connect(timer, croupier.getNegative(Timer.class));
        connect(network , croupier.getNegative(Network.class), new IntegerOverlayFilter(0));
        connect(croupier.getPositive(CroupierPort.class), pseudoGradient.getNegative(CroupierPort.class));

        subscribe(handleCroupierDisconnect, croupier.getPositive(CroupierControlPort.class));
        log.debug("expecting start croupier next");
    }

    private Handler<CroupierDisconnected> handleCroupierDisconnect = new Handler<CroupierDisconnected>() {

        @Override
        public void handle(CroupierDisconnected event) {
            log.error("croupier disconnected .. ");
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


    // =====
    //  Simulator Event Handlers.
    // =====


    ClassMatchedHandler<AddIndexEntryP2pSimulated, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, AddIndexEntryP2pSimulated>> addEntrySimulatorHandler = new ClassMatchedHandler<AddIndexEntryP2pSimulated, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, AddIndexEntryP2pSimulated>>() {
        @Override
        public void handle(AddIndexEntryP2pSimulated request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, AddIndexEntryP2pSimulated> event) {
            log.debug("{}: Add Entry Received for Node:", self.getId());
            trigger(new SimulationEventsPort.AddIndexSimulated(request.getIndexEntry()), search.getNegative(SimulationEventsPort.class));
        }
    };


    DecoratedAddress currentSimAddress = null;

    ClassMatchedHandler<SearchP2pSimulated.Request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, SearchP2pSimulated.Request>> searchSimulatorHandler =
            new ClassMatchedHandler<SearchP2pSimulated.Request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, SearchP2pSimulated.Request>>() {

                @Override
                public void handle(SearchP2pSimulated.Request request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, SearchP2pSimulated.Request> event) {

                    currentSimAddress = event.getSource();
                    log.debug("Search Event Received : {}", request.getSearchPattern());
                    trigger(new SimulationEventsPort.SearchSimulated.Request(request.getSearchPattern(), request.getSearchTimeout(), request.getFanoutParameter()), search.getNegative(SimulationEventsPort.class));
                }
            };

    /**
     * Handler for the Response from the Search Component regarding the responses and the partitions
     * hit for the request.
     */
    Handler<SimulationEventsPort.SearchSimulated.Response> searchSimulatedResponseHandler = new Handler<SimulationEventsPort.SearchSimulated.Response>() {
        @Override
        public void handle(SimulationEventsPort.SearchSimulated.Response event) {

            log.debug("{}: Received the search simulated response from the child component", self.getId());
            if(currentSimAddress != null){
                log.debug("Responses: {}, Partitions Hit: {}", event.getResponses(), event.getPartitionHit());
                SearchP2pSimulated.Response response = new SearchP2pSimulated.Response(event.getResponses(), event.getPartitionHit());
                trigger(CommonHelper.getDecoratedContentMessage(self.getAddress(), currentSimAddress, Transport.UDP, response), network);
            }
        }
    };



}
