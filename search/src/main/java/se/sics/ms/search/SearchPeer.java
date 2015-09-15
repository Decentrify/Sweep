package se.sics.ms.search;

import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.config.*;
import se.sics.kompics.*;
import se.sics.kompics.network.Network;
import se.sics.kompics.network.Transport;
import se.sics.kompics.timer.Timer;
import se.sics.ktoolbox.aggregator.client.LocalAggregator;
import se.sics.ktoolbox.aggregator.client.LocalAggregatorInit;
import se.sics.ktoolbox.aggregator.client.LocalAggregatorPort;
import se.sics.ktoolbox.aggregator.client.util.ComponentInfoProcessor;
import se.sics.ktoolbox.cc.heartbeat.CCHeartbeatPort;
import se.sics.ktoolbox.cc.heartbeat.msg.CCHeartbeat;
import se.sics.ktoolbox.cc.heartbeat.msg.CCOverlaySample;
import se.sics.ms.data.aggregator.SearchComponentInfo;
import se.sics.ms.data.aggregator.processor.CompInternalStateProcessor;
import se.sics.ms.common.ApplicationSelf;
import se.sics.ms.configuration.MsConfig;
import se.sics.ms.events.*;
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
import se.sics.ms.types.PeerDescriptor;

import java.security.*;
import java.util.*;

import se.sics.ms.util.CommonHelper;
import se.sics.ms.util.ComparatorCollection;
import se.sics.ms.util.HeartbeatServiceEnum;
import se.sics.ms.util.TimeoutCollection;
import se.sics.p2ptoolbox.chunkmanager.ChunkManagerComp;
import se.sics.p2ptoolbox.chunkmanager.ChunkManagerConfig;
import se.sics.p2ptoolbox.croupier.CroupierComp;
import se.sics.p2ptoolbox.croupier.CroupierConfig;
import se.sics.p2ptoolbox.croupier.CroupierControlPort;
import se.sics.p2ptoolbox.croupier.CroupierPort;
import se.sics.p2ptoolbox.croupier.msg.CroupierDisconnected;
import se.sics.p2ptoolbox.croupier.msg.CroupierJoin;
import se.sics.p2ptoolbox.election.api.ports.LeaderElectionPort;
import se.sics.p2ptoolbox.election.core.ElectionConfig;
import se.sics.p2ptoolbox.election.core.ElectionFollower;
import se.sics.p2ptoolbox.election.core.ElectionInit;
import se.sics.p2ptoolbox.election.core.ElectionLeader;
import se.sics.p2ptoolbox.gradient.GradientComp;
import se.sics.p2ptoolbox.gradient.GradientConfig;
import se.sics.p2ptoolbox.gradient.GradientPort;
import se.sics.p2ptoolbox.gradient.msg.GradientUpdate;
import se.sics.p2ptoolbox.tgradient.TreeGradientComp;
import se.sics.p2ptoolbox.tgradient.TreeGradientConfig;
import se.sics.p2ptoolbox.util.config.SystemConfig;

import se.sics.p2ptoolbox.util.filters.IntegerOverlayFilter;
import se.sics.p2ptoolbox.util.network.impl.BasicContentMsg;
import se.sics.p2ptoolbox.util.network.impl.DecoratedAddress;
import se.sics.p2ptoolbox.util.network.impl.DecoratedHeader;
import se.sics.p2ptoolbox.util.update.SelfAddressUpdate;
import se.sics.p2ptoolbox.util.update.SelfAddressUpdatePort;
import se.sics.p2ptoolbox.util.update.SelfViewUpdatePort;
import se.sics.util.*;

public final class SearchPeer extends ComponentDefinition {

    private static final Logger log = LoggerFactory.getLogger(SearchPeer.class);

    Positive<SimulationEventsPort> indexPort = positive(SimulationEventsPort.class);
    Positive<Network> network = positive(Network.class);
    Positive<Timer> timer = positive(Timer.class);
    Negative<UiPort> internalUiPort = negative(UiPort.class);
    Positive<UiPort> externalUiPort = positive(UiPort.class);
    Positive<CCHeartbeatPort> heartbeatPort = requires(CCHeartbeatPort.class);
    Positive<SelfAddressUpdatePort> selfAddressUpdatePort = requires(SelfAddressUpdatePort.class);
    Positive<SelfViewUpdatePort> selfViewUpdatePort = requires(SelfViewUpdatePort.class);

    private Component croupier;
    private Component gradient, tgradient;
    private Component search, aggregatorComponent, routing;
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

        pseudoGradientConfiguration = init.getPseudoGradientConfiguration();
        searchConfiguration = init.getSearchConfiguration();
        chunkManagerConfig = init.getChunkManagerConfig();
        systemConfig = init.getSystemConfig();

        //      Build application self here.
        self = new ApplicationSelf(systemConfig.self);

        routing = create(Routing.class, new RoutingInit(systemConfig.seed, self, pseudoGradientConfiguration));
        search = create(NPAwareSearch.class, new SearchInit(systemConfig.seed, self, searchConfiguration, publicKey, privateKey));

        // External Components creating and connection to the local components.
        connectCroupier(init.getCroupierConfiguration());
        connectGradient(init.getGradientConfig(), systemConfig.seed);
        connectTreeGradient(init.getTGradientConfig(), init.getGradientConfig());
        connectElection(init.getElectionConfig(), systemConfig.seed);
        connectAggregator(systemConfig);

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

        subscribe(paginateSearchRequestHandler, externalUiPort);
        subscribe(paginateSearchResponseHandler, search.getPositive(UiPort.class));

        subscribe(overlaySampleResponseHandler, heartbeatPort);
        subscribe(croupierDisconnectedHandler, croupier.getPositive(CroupierControlPort.class));
        subscribe(caracalTimeoutHandler, timer);
    }


    /**
     * Main method indicating the bootstrapping of multiple services.
     * At the moment, only croupier service needs to be bootstrapped but in future
     * many services can be bootstrapped.
     *
     */
    private void initiateServiceBootstrapping(){

        log.info("Going to initiate bootstrapping all the services.");

//      Before bootstrapping inform caracal through heart beats.
        byte[] croupierService = getCroupierServiceByteArray();
        log.debug("Triggering the heart beat to the caracal service with overlay :{}.", croupierService);

        trigger(new CCHeartbeat.Start(croupierService), heartbeatPort);
        initiateCroupierServiceBootstrap();
    }


    Handler<TimeoutCollection.CaracalTimeout> caracalTimeoutHandler = new Handler<TimeoutCollection.CaracalTimeout>() {
        @Override
        public void handle(TimeoutCollection.CaracalTimeout caracalTimeout) {

            log.info("Initiating the croupier service bootstrap");
            initiateCroupierServiceBootstrap();
        }
    };

    /**
     * Request for the bootstrapping nodes from the caracal.
     */
    private void initiateCroupierServiceBootstrap(){

        log.debug("Trying to connect to caracal for fetching the bootstrapping nodes.");

//      CONSTRUCT AND SEND THE BYTE ARRAY AND THEN INT.
        byte[] croupierServiceByteArray = getCroupierServiceByteArray();
        log.debug("Croupier Service Byte Array: {}", croupierServiceByteArray);

        trigger(new CCOverlaySample.Request(croupierServiceByteArray), heartbeatPort);

    }

    /**
     * Overlay Sample Response Handler.
     * FIX ME: Handle the response generically. For now as only
     * one service needs to be bootstrapped therefore it could be allowed.
     *
     */
    Handler<CCOverlaySample.Response> overlaySampleResponseHandler = new Handler<CCOverlaySample.Response>() {
        @Override
        public void handle(CCOverlaySample.Response response) {

            log.debug("Received overlay sample response for croupier now.");

            byte[] croupierService = getCroupierServiceByteArray();
            byte[] receivedArray  = response.overlayId;

            if(!Arrays.equals(croupierService, receivedArray)){
                log.warn("Received caracal service response for an unknown service.");
                return;
            }

//          Now you actually launch the search peer.
            List<DecoratedAddress> bootstrapSet = new ArrayList<DecoratedAddress>(response.overlaySample);

//          Bootstrap the croupier service.
            trigger(new CroupierJoin(bootstrapSet), croupier.getPositive(CroupierControlPort.class));

        }
    };

    /**
     * Get the byte array for the croupier service.
     * This byte array will be used to bootstrap the croupier
     * with the sample.
     *
     * @return byte array.
     */
    private byte[] getCroupierServiceByteArray(){

        byte[] overlayByteArray = Ints.toByteArray(MsConfig.CROUPIER_OVERLAY_ID);
        byte[] resultByteArray = new byte[] { HeartbeatServiceEnum.CROUPIER.getServiceId(),
                overlayByteArray[1],
                overlayByteArray[2],
                overlayByteArray[3]};

        return resultByteArray;
    }

    /**
     * Main handler to be executed when the croupier disconnected
     * is received by the application.
     */
    Handler<CroupierDisconnected> croupierDisconnectedHandler = new Handler<CroupierDisconnected>() {
        @Override
        public void handle(CroupierDisconnected croupierDisconnected) {

            log.info("Croupier disconnected. Requesting caracal for bootstrap nodes.");
            retryCroupierServiceBootstrap();
        }
    };


    /**
     * Involves any cleaning up to be performed before the
     * croupier service can be re-bootstrapped
     */
    private void retryCroupierServiceBootstrap(){
        log.debug("Going to reconnect the caracal for bootstrapping the croupier.");
        initiateCroupierServiceBootstrap();
    }


        // Gradient Port Connections.

    /**
     * Perform the internal connections among the components
     * that are local to the application. In other words, connect the components
     */
    private void doInternalConnections(){
        
        // Timer Connections.
        connect(timer, search.getNegative(Timer.class));
        connect(timer, routing.getNegative(Timer.class));

        // Internal Connections.

        connect(indexPort, search.getNegative(SimulationEventsPort.class));
        connect(search.getPositive(LeaderStatusPort.class), routing.getNegative(LeaderStatusPort.class));
        connect(routing.getPositive(GradientRoutingPort.class), search.getNegative(GradientRoutingPort.class));
        connect(internalUiPort, search.getPositive(UiPort.class));
        connect(search.getPositive(SelfChangedPort.class), routing.getNegative(SelfChangedPort.class));
        
    }


    /**
     * Make the connections to the local aggregator in the system.
     * @param systemConfig system configuration.
     */
    private void connectAggregator(SystemConfig systemConfig){

        log.debug("Initiating the connection to the local aggregator component.");

        if(!systemConfig.aggregator.isPresent()){
            log.warn("Unable to bootup local aggregator component as the information about the global aggregator missing.");
//            return;
        }

        DecoratedAddress globalAggregatorAddress = systemConfig.aggregator.isPresent()? systemConfig.aggregator.get() : null;
        DecoratedAddress selfAddress = systemConfig.self;

        Map<Class, List<ComponentInfoProcessor>> componentProcessorMap = getComponentProcessorMap();
        Component aggregator = create(LocalAggregator.class, new LocalAggregatorInit( MsConfig.LOCAL_AGGREGATOR_TIMEOUT, componentProcessorMap,
                globalAggregatorAddress, selfAddress ));
        connect(timer, aggregator.getNegative(Timer.class));
        connect(network, aggregator.getNegative(Network.class));
        connect(aggregator.getPositive(LocalAggregatorPort.class), search.getNegative(LocalAggregatorPort.class));

    }


    /**
     * Construct the component information processor map,
     * which will be used by the aggregator in task to create packets to be sent to
     * the global aggregator.
     *
     * @return ProcessorMap.
     */
    private Map<Class, List<ComponentInfoProcessor>> getComponentProcessorMap(){

        Map<Class, List<ComponentInfoProcessor>> componentProcessorMap = new HashMap<Class, List<ComponentInfoProcessor>>();

        List<ComponentInfoProcessor> searchCompProcessors = new ArrayList<ComponentInfoProcessor>();
        searchCompProcessors.add(new CompInternalStateProcessor());
        componentProcessorMap.put(SearchComponentInfo.class, searchCompProcessors);     // Processor list for the search component information.

        return componentProcessorMap;
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
        connect(partitionAwareGradient.getPositive(GradientPort.class), routing.getNegative(GradientPort.class));
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

            log.error("Start Event Received, now initiating the bootstrapping .... ");
            initiateServiceBootstrapping();

            startGradient();
        }
    };

    /**
     * Connect the application with the leader election protocol.
     *
     * @param electionConfig Election Configuration
     * @param seed seed
     */
    private void connectElection(ElectionConfig electionConfig, long seed) {

        log.info("Creating application specific rule set.");

        LEContainerComparator containerComparator = new LEContainerComparator(new SimpleLCPViewComparator(), new ComparatorCollection.AddressComparator());
        ApplicationRuleSet.SweepLCRuleSet leaderComponentRuleSet = new ApplicationRuleSet.SweepLCRuleSet(containerComparator);
        ApplicationRuleSet.SweepCohortsRuleSet cohortsRuleSet = new ApplicationRuleSet.SweepCohortsRuleSet(containerComparator);

        log.info("Starting with the election components creation and connections.");

        electionLeader = create(ElectionLeader.class, new ElectionInit<ElectionLeader>(
                self.getAddress(),
                new PeerDescriptor(self.getAddress()),
                seed,
                electionConfig,
                publicKey,
                privateKey,
                new SimpleLCPViewComparator(),
                leaderComponentRuleSet,
                cohortsRuleSet));

        electionFollower = create(ElectionFollower.class, new ElectionInit<ElectionFollower>(
                        self.getAddress(),
                        new PeerDescriptor(self.getAddress()),
                        seed,            // Bootstrap the underlying services.
                        electionConfig,
                        publicKey,
                        privateKey,
                        new SimpleLCPViewComparator(),
                        leaderComponentRuleSet,
                        cohortsRuleSet));

        // Election leader connections.
        connect(network, electionLeader.getNegative(Network.class));
        connect(timer, electionLeader.getNegative(Timer.class));
        connect(gradient.getPositive(GradientPort.class), electionLeader.getNegative(GradientPort.class));
        connect(electionLeader.getPositive(LeaderElectionPort.class), search.getNegative(LeaderElectionPort.class));
        connect(electionLeader.getPositive(LeaderElectionPort.class), routing.getNegative(LeaderElectionPort.class));
        
        // Election follower connections.
        connect(network, electionFollower.getNegative(Network.class));
        connect(timer, electionFollower.getNegative(Timer.class));
        connect(gradient.getPositive(GradientPort.class), electionFollower.getNegative(GradientPort.class));
        connect(electionFollower.getPositive(LeaderElectionPort.class), search.getNegative(LeaderElectionPort.class));
        connect(electionFollower.getPositive(LeaderElectionPort.class), routing.getNegative(LeaderElectionPort.class));
    }


	/**
     * Initialize the PAG service.
     */
    private void startPAG(){
        log.debug("Sending initial self update to the PAG ... ");
        trigger(new PAGUpdate(new PeerDescriptor(self.getAddress())), partitionAwareGradient.getPositive(PAGPort.class));
        trigger(new GradientUpdate<PeerDescriptor>(new PeerDescriptor(self.getAddress())), partitionAwareGradient.getPositive(GradientPort.class));
    }


    /**
     * Connect gradient with the application.
     * @param gradientConfig System's Gradient Configuration.
     * @param seed Seed for the Random Generator.
     */
    private void connectGradient(GradientConfig gradientConfig, long seed) {
        
        log.info("connecting gradient configuration ...");
        gradient = create(GradientComp.class, new GradientComp.GradientInit(systemConfig, gradientConfig, MsConfig.GRADIENT_OVERLAY_ID , new SimpleUtilityComparator(), new SweepGradientFilter()));
        connect(network, gradient.getNegative(Network.class), new IntegerOverlayFilter(MsConfig.GRADIENT_OVERLAY_ID));
        connect(timer, gradient.getNegative(Timer.class));
        connect(croupier.getPositive(CroupierPort.class), gradient.getNegative(CroupierPort.class));
        connect(croupier.getNegative(SelfViewUpdatePort.class), gradient.getPositive(SelfViewUpdatePort.class));
        connect(gradient.getNegative(SelfAddressUpdatePort.class), selfAddressUpdatePort);

    }

    /**
     * Connect gradient with the application.
     * @param gradientConfig System's Gradient Configuration.
     */
    private void connectTreeGradient(TreeGradientConfig tgradientConfig, GradientConfig gradientConfig) {

        log.info("connecting tree gradient configuration ...");
        tgradient = create(TreeGradientComp.class, new TreeGradientComp.TreeGradientInit(systemConfig, gradientConfig, tgradientConfig, MsConfig.T_GRADIENT_OVERLAY_ID , new SweepGradientFilter()));
        connect(network, tgradient.getNegative(Network.class), new IntegerOverlayFilter(MsConfig.T_GRADIENT_OVERLAY_ID));
        connect(timer, tgradient.getNegative(Timer.class));
        connect(croupier.getPositive(CroupierPort.class), tgradient.getNegative(CroupierPort.class));
        connect(gradient.getPositive(GradientPort.class), tgradient.getNegative(GradientPort.class));
        connect(search.getNegative(GradientPort.class), tgradient.getPositive(GradientPort.class));
        connect(routing.getNegative(GradientPort.class), tgradient.getPositive(GradientPort.class));
        connect(gradient.getNegative(SelfViewUpdatePort.class), tgradient.getPositive(SelfViewUpdatePort.class));
        connect(tgradient.getNegative(SelfViewUpdatePort.class), selfViewUpdatePort);
        connect(tgradient.getNegative(SelfAddressUpdatePort.class), selfAddressUpdatePort);

    }

    /**
     * Boot Up the gradient service.
     */
    private void startGradient() {
        log.info("Starting Gradient component.");
        trigger(new GradientUpdate<PeerDescriptor>(new PeerDescriptor(self.getAddress())), tgradient.getPositive(GradientPort.class));
    }
    
    
    private void connectCroupier( CroupierConfig config ) {

        log.info("connecting croupier components...");
        int croupierOverlay = getCroupierServiceInt(getCroupierServiceByteArray());

        croupier = create(CroupierComp.class, new CroupierComp.CroupierInit(systemConfig, config, croupierOverlay));
        connect(timer, croupier.getNegative(Timer.class));
        connect(network, croupier.getNegative(Network.class), new IntegerOverlayFilter(croupierOverlay));
        connect(croupier.getPositive(CroupierPort.class), routing.getNegative(CroupierPort.class));
        connect(croupier.getNegative(SelfAddressUpdatePort.class), selfAddressUpdatePort);

        subscribe(handleCroupierDisconnect, croupier.getPositive(CroupierControlPort.class));
        log.debug("expecting start croupier next");
    }

    private int getCroupierServiceInt(byte[] serviceArray){
        return Ints.fromByteArray(serviceArray);
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


    final Handler<se.sics.ms.events.paginateAware.UiSearchRequest> paginateSearchRequestHandler = new Handler<se.sics.ms.events.paginateAware.UiSearchRequest>() {
        @Override
        public void handle(se.sics.ms.events.paginateAware.UiSearchRequest uiSearchRequest) {

            log.debug("Received search request from application.");
            trigger(uiSearchRequest, search.getPositive(UiPort.class));
        }
    };


    final Handler<se.sics.ms.events.paginateAware.UiSearchResponse> paginateSearchResponseHandler = new Handler<se.sics.ms.events.paginateAware.UiSearchResponse>() {
        @Override
        public void handle(se.sics.ms.events.paginateAware.UiSearchResponse uiSearchResponse) {

            log.debug("Received search response from the application.");
            trigger(uiSearchResponse, externalUiPort);
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
