package se.sics.ms.search;

import se.sics.ms.util.SimpleLCPViewComparator;
import se.sics.ms.util.ApplicationRuleSet;
import se.sics.ms.util.LEContainerComparator;
import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.*;
import se.sics.kompics.network.Network;
import se.sics.kompics.network.Transport;
import se.sics.kompics.timer.Timer;
import se.sics.ktoolbox.cc.heartbeat.CCHeartbeatPort;
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
import se.sics.ktoolbox.cc.heartbeat.event.CCHeartbeat;
import se.sics.ktoolbox.cc.heartbeat.event.CCOverlaySample;
import se.sics.ktoolbox.chunkmanager.ChunkManagerComp;
import se.sics.ktoolbox.chunkmanager.ChunkManagerConfig;
import se.sics.ktoolbox.croupier.CroupierComp;
import se.sics.ktoolbox.croupier.CroupierControlPort;
import se.sics.ktoolbox.croupier.CroupierPort;
import se.sics.ktoolbox.croupier.event.CroupierDisconnected;
import se.sics.ktoolbox.croupier.event.CroupierJoin;
import se.sics.ktoolbox.election.ElectionConfig;
import se.sics.ktoolbox.election.ElectionFollower;
import se.sics.ktoolbox.election.ElectionInit;
import se.sics.ktoolbox.election.ElectionLeader;
import se.sics.ktoolbox.election.api.ports.LeaderElectionPort;
import se.sics.ktoolbox.gradient.GradientComp;
import se.sics.ktoolbox.gradient.GradientPort;
import se.sics.ktoolbox.gradient.temp.RankUpdatePort;
import se.sics.ktoolbox.tgradient.TreeGradientComp;
import se.sics.ktoolbox.util.address.AddressUpdatePort;
import se.sics.ktoolbox.util.config.impl.SystemKCWrapper;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.basic.IntIdentifier;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.KHeader;
import se.sics.ktoolbox.util.network.basic.BasicContentMsg;
import se.sics.ktoolbox.util.selectors.OverlaySelector;
import se.sics.ktoolbox.util.update.view.OverlayViewUpdate;
import se.sics.ktoolbox.util.update.view.ViewUpdatePort;
import se.sics.ms.gvod.config.SearchConfiguration;

import se.sics.ms.util.CommonHelper;
import se.sics.ms.util.ComparatorCollection;
import se.sics.ms.util.HeartbeatServiceEnum;
import se.sics.ms.util.TimeoutCollection;

public final class SearchPeer extends ComponentDefinition {

    private static final Logger log = LoggerFactory.getLogger(SearchPeer.class);

    Positive<SimulationEventsPort> indexPort = positive(SimulationEventsPort.class);
    Positive<Network> network = positive(Network.class);
    Positive<Timer> timer = positive(Timer.class);
    Negative<UiPort> internalUiPort = negative(UiPort.class);
    Positive<UiPort> externalUiPort = positive(UiPort.class);
    Positive<CCHeartbeatPort> heartbeatPort = requires(CCHeartbeatPort.class);
    Positive<AddressUpdatePort> selfAddressUpdatePort = requires(AddressUpdatePort.class);
//    Positive<SelfViewUpdatePort> selfViewUpdatePort = requires(SelfViewUpdatePort.class);

    private Component chunkManager;
    private Component croupier;
    private Component gradient, tgradient;
    private Component search, aggregatorComponent, routing;
    private Component partitionAwareGradient;
    private Component electionLeader, electionFollower;
    private ApplicationSelf self;
    private SearchConfiguration searchConfiguration;
    private final SystemKCWrapper systemConfig;

    private PublicKey publicKey;
    private PrivateKey privateKey;

    public SearchPeer(SearchPeerInit init) throws NoSuchAlgorithmException {

        // Generate the Key Pair to be used by the application.
        generateKeys();

        //      Build application self here.
        self = new ApplicationSelf(init.self);
        searchConfiguration = init.searchConfig;
        systemConfig = new SystemKCWrapper(config());

        routing = create(Routing.class, new RoutingInit(systemConfig.seed, self, init.gradientConfig));
        search = create(NPAwareSearch.class, new SearchInit(systemConfig.seed, self, searchConfiguration, publicKey, privateKey));

        // External Components creating and connection to the local components.
        connectChunkManager();
        connectCroupier();
        connectGradient();
        connectTreeGradient();
        connectElection(init.electionConfig, systemConfig.seed);
        //TODO Alex - aggregator
//        connectAggregator(systemConfig);

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
     * Main method indicating the bootstrapping of multiple services. At the
     * moment, only croupier service needs to be bootstrapped but in future many
     * services can be bootstrapped.
     *
     */
    private void initiateServiceBootstrapping() {

        log.info("Going to initiate bootstrapping all the services.");

//      Before bootstrapping inform caracal through heart beats.
        Identifier croupierServiceId = new IntIdentifier(Ints.fromByteArray(getCroupierServiceByteArray()));
        log.debug("Triggering the heart beat to the caracal service with overlay :{}.", croupierServiceId);

        trigger(new CCHeartbeat.Start(croupierServiceId), heartbeatPort);
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
    private void initiateCroupierServiceBootstrap() {

        log.debug("Trying to connect to caracal for fetching the bootstrapping nodes.");

//      CONSTRUCT AND SEND THE BYTE ARRAY AND THEN INT.
        Identifier croupierServiceId = new IntIdentifier(Ints.fromByteArray(getCroupierServiceByteArray()));
        log.debug("Croupier Service Id: {}", croupierServiceId);

        trigger(new CCOverlaySample.Request(croupierServiceId), heartbeatPort);

    }

    /**
     * Overlay Sample Response Handler. FIX ME: Handle the response generically.
     * For now as only one service needs to be bootstrapped therefore it could
     * be allowed.
     *
     */
    Handler<CCOverlaySample.Response> overlaySampleResponseHandler = new Handler<CCOverlaySample.Response>() {
        @Override
        public void handle(CCOverlaySample.Response response) {

            log.debug("Received overlay sample response for croupier now.");

            Identifier croupierService = new IntIdentifier(Ints.fromByteArray(getCroupierServiceByteArray()));
            Identifier receivedArray = response.req.overlayId;

            if (!croupierService.equals(receivedArray)) {
                log.warn("Received caracal service response for an unknown service.");
                return;
            }

//          Now you actually launch the search peer.
            List<KAddress> bootstrapSet = new ArrayList<KAddress>(response.overlaySample);
            log.debug("{}: The size of bootstrap set : {}", self.getId(), bootstrapSet);

//          Bootstrap the croupier service.
            trigger(new CroupierJoin(bootstrapSet), croupier.getPositive(CroupierControlPort.class));

        }
    };

    /**
     * Get the byte array for the croupier service. This byte array will be used
     * to bootstrap the croupier with the sample.
     *
     * @return byte array.
     */
    private byte[] getCroupierServiceByteArray() {

        byte[] overlayByteArray = Ints.toByteArray(MsConfig.CROUPIER_OVERLAY_ID);
        byte[] resultByteArray = new byte[]{HeartbeatServiceEnum.CROUPIER.getServiceId(),
            overlayByteArray[1],
            overlayByteArray[2],
            overlayByteArray[3]};

        return resultByteArray;
    }

    /**
     * Main handler to be executed when the croupier disconnected is received by
     * the application.
     */
    Handler<CroupierDisconnected> croupierDisconnectedHandler = new Handler<CroupierDisconnected>() {
        @Override
        public void handle(CroupierDisconnected croupierDisconnected) {

            log.info("Croupier disconnected. Requesting caracal for bootstrap nodes.");
            retryCroupierServiceBootstrap();
        }
    };

    /**
     * Involves any cleaning up to be performed before the croupier service can
     * be re-bootstrapped
     */
    private void retryCroupierServiceBootstrap() {
        log.debug("Going to reconnect the caracal for bootstrapping the croupier.");
        initiateCroupierServiceBootstrap();
    }

    // Gradient Port Connections.
    /**
     * Perform the internal connections among the components that are local to
     * the application. In other words, connect the components
     */
    private void doInternalConnections() {

        // Timer Connections.
        connect(timer, search.getNegative(Timer.class), Channel.TWO_WAY);
        connect(timer, routing.getNegative(Timer.class), Channel.TWO_WAY);

        // Internal Connections.
        connect(chunkManager.getPositive(Network.class), search.getNegative(Network.class), Channel.TWO_WAY);
        connect(chunkManager.getPositive(Network.class), routing.getNegative(Network.class), Channel.TWO_WAY);

//        connect(network, search.getNegative(Network.class), Channel.TWO_WAY);
//        connect(network, routing.getNegative(Network.class), Channel.TWO_WAY);
        connect(indexPort, search.getNegative(SimulationEventsPort.class), Channel.TWO_WAY);
        connect(search.getPositive(LeaderStatusPort.class), routing.getNegative(LeaderStatusPort.class), Channel.TWO_WAY);
        connect(routing.getPositive(GradientRoutingPort.class), search.getNegative(GradientRoutingPort.class), Channel.TWO_WAY);
        connect(internalUiPort, search.getPositive(UiPort.class), Channel.TWO_WAY);
        connect(search.getPositive(SelfChangedPort.class), routing.getNegative(SelfChangedPort.class), Channel.TWO_WAY);

    }

    private void connectChunkManager() {
        
        //TODO Alex - remove hardocoded values;
        chunkManager = create(ChunkManagerComp.class, new ChunkManagerComp.CMInit(new ChunkManagerConfig(30000, 1000), self.getAddress()));
        connect(chunkManager.getNegative(Network.class), network, Channel.TWO_WAY);
        connect(chunkManager.getNegative(Timer.class), timer, Channel.TWO_WAY);
    }

    /**
     * Make the connections to the local aggregator in the system.
     *
     * @param systemConfig system configuration.
     */
    private void connectAggregator() {

        log.debug("Initiating the connection to the local aggregator component.");

        if (!systemConfig.aggregator.isPresent()) {
            log.warn("Unable to bootup local aggregator component as the information about the global aggregator missing.");
            return;
        }

        KAddress globalAggregatorAddress = systemConfig.aggregator.get();
        KAddress selfAddress = self.getAddress();

        //TODO Alex - start aggregator
//        Map<Class, List<ComponentInfoProcessor>> componentProcessorMap = getComponentProcessorMap();
//        Component aggregator = create(LocalAggregator.class, new LocalAggregatorInit( MsConfig.AGGREGATOR_TIMEOUT, componentProcessorMap,
//                globalAggregatorAddress, selfAddress ));
//        connect(timer, aggregator.getNegative(Timer.class));
//        connect(network, aggregator.getNegative(Network.class));
//        connect(aggregator.getPositive(LocalAggregatorPort.class), search.getNegative(LocalAggregatorPort.class));
//        connect(aggregator.getNegative(SelfAddressUpdatePort.class), selfAddressUpdatePort);
    }

//    /**
//     * Construct the component information processor map,
//     * which will be used by the aggregator in task to create packets to be sent to
//     * the global aggregator.
//     *
//     * @return ProcessorMap.
//     */
//    private Map<Class, List<ComponentInfoProcessor>> getComponentProcessorMap(){
//
//        Map<Class, List<ComponentInfoProcessor>> componentProcessorMap = new HashMap<Class, List<ComponentInfoProcessor>>();
//
//        List<ComponentInfoProcessor> searchCompProcessors = new ArrayList<ComponentInfoProcessor>();
//        searchCompProcessors.add(new CompInternalStateProcessor());
//        componentProcessorMap.put(SearchComponentInfo.class, searchCompProcessors);     // Processor list for the search component information.
//
//        return componentProcessorMap;
//    }
    //TODO Alex - end aggregator
    /**
     * Connect the application with the partition aware gradient. The PAG will
     * enclose the gradient, so connection to the PAG will be similar to the
     * gradient.
     *
     * @param systemConfig system configuration.
     * @param gradientConfig gradient configuration.
     */
    private void connectPAG() {

        log.debug("Initiating the connection to the partition aware gradient.");

        //TODO Alex - PAG identifier
        Identifier pagId = new IntIdentifier(0);
        PAGInit init = new PAGInit(config(), self.getAddress(), pagId, 50);
        partitionAwareGradient = create(PartitionAwareGradient.class, init);

        connect(partitionAwareGradient.getNegative(Timer.class), timer, Channel.TWO_WAY);
        connect(network, partitionAwareGradient.getNegative(Network.class), new OverlaySelector(pagId, true), Channel.TWO_WAY);
        connect(croupier.getPositive(CroupierPort.class), partitionAwareGradient.getNegative(CroupierPort.class), Channel.TWO_WAY);
        connect(partitionAwareGradient.getPositive(PAGPort.class), search.getNegative(PAGPort.class), Channel.TWO_WAY);
        connect(partitionAwareGradient.getPositive(GradientPort.class), search.getNegative(GradientPort.class), Channel.TWO_WAY);
        connect(partitionAwareGradient.getPositive(GradientPort.class), routing.getNegative(GradientPort.class), Channel.TWO_WAY);
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

            log.debug("Start Event Received, now initiating the bootstrapping .... ");
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
                seed, // Bootstrap the underlying services.
                electionConfig,
                publicKey,
                privateKey,
                new SimpleLCPViewComparator(),
                leaderComponentRuleSet,
                cohortsRuleSet));

        // Election leader connections.
        connect(network, electionLeader.getNegative(Network.class), Channel.TWO_WAY);
        connect(timer, electionLeader.getNegative(Timer.class), Channel.TWO_WAY);
        connect(gradient.getPositive(GradientPort.class), electionLeader.getNegative(GradientPort.class), Channel.TWO_WAY);
        connect(electionLeader.getPositive(LeaderElectionPort.class), search.getNegative(LeaderElectionPort.class), Channel.TWO_WAY);
        connect(electionLeader.getPositive(LeaderElectionPort.class), routing.getNegative(LeaderElectionPort.class), Channel.TWO_WAY);

        // Election follower connections.
        connect(network, electionFollower.getNegative(Network.class), Channel.TWO_WAY);
        connect(timer, electionFollower.getNegative(Timer.class), Channel.TWO_WAY);
        connect(gradient.getPositive(GradientPort.class), electionFollower.getNegative(GradientPort.class), Channel.TWO_WAY);
        connect(electionFollower.getPositive(LeaderElectionPort.class), search.getNegative(LeaderElectionPort.class), Channel.TWO_WAY);
        connect(electionFollower.getPositive(LeaderElectionPort.class), routing.getNegative(LeaderElectionPort.class), Channel.TWO_WAY);
    }

    /**
     * Initialize the PAG service.
     */
    private void startPAG() {
        log.debug("Sending initial self update to the PAG ... ");
        trigger(new PAGUpdate(new PeerDescriptor(self.getAddress())), partitionAwareGradient.getPositive(PAGPort.class));
        //TODO Alex WARN - does pag need connection to view update?
//        trigger(new OverlayViewUpdate.Indication<PeerDescriptor>(new PeerDescriptor(self.getAddress())), partitionAwareGradient.getPositive(GradientPort.class));
    }

    /**
     * Connect gradient with the application.
     *
     * @param gradientConfig System's Gradient Configuration.
     * @param seed Seed for the Random Generator.
     */
    private void connectGradient() {

        log.info("connecting gradient configuration ...");
        Identifier gradientId = new IntIdentifier(MsConfig.GRADIENT_OVERLAY_ID);
        gradient = create(GradientComp.class, new GradientComp.GradientInit(self.getAddress(), new SimpleUtilityComparator(),
                new SweepGradientFilter(), systemConfig.seed, gradientId));
        connect(network, gradient.getNegative(Network.class), new OverlaySelector(gradientId, true), Channel.TWO_WAY);
        connect(timer, gradient.getNegative(Timer.class), Channel.TWO_WAY);
        connect(croupier.getPositive(CroupierPort.class), gradient.getNegative(CroupierPort.class), Channel.TWO_WAY);
        connect(croupier.getNegative(ViewUpdatePort.class), gradient.getPositive(ViewUpdatePort.class), Channel.TWO_WAY);
        connect(gradient.getNegative(AddressUpdatePort.class), selfAddressUpdatePort, Channel.TWO_WAY);

    }

    /**
     * Connect gradient with the application.
     *
     * @param gradientConfig System's Gradient Configuration.
     */
    private void connectTreeGradient() {

        log.info("connecting tree gradient configuration ...");
        Identifier tGradientId = new IntIdentifier(MsConfig.T_GRADIENT_OVERLAY_ID);
        tgradient = create(TreeGradientComp.class, new TreeGradientComp.TreeGradientInit(self.getAddress(),
                new SweepGradientFilter(), systemConfig.seed, tGradientId));
        connect(tgradient.getNegative(Network.class), network, new OverlaySelector(tGradientId, true), Channel.TWO_WAY);
        connect(tgradient.getNegative(Timer.class), timer, Channel.TWO_WAY);
        connect(tgradient.getNegative(CroupierPort.class), croupier.getPositive(CroupierPort.class), Channel.TWO_WAY);
        connect(gradient.getPositive(GradientPort.class), tgradient.getNegative(GradientPort.class), Channel.TWO_WAY);
        connect(search.getNegative(GradientPort.class), tgradient.getPositive(GradientPort.class), Channel.TWO_WAY);
        connect(routing.getNegative(GradientPort.class), tgradient.getPositive(GradientPort.class), Channel.TWO_WAY);
        connect(gradient.getNegative(ViewUpdatePort.class), tgradient.getPositive(ViewUpdatePort.class), Channel.TWO_WAY);
        connect(tgradient.getNegative(ViewUpdatePort.class), search.getPositive(ViewUpdatePort.class), Channel.TWO_WAY);
        connect(tgradient.getNegative(AddressUpdatePort.class), selfAddressUpdatePort, Channel.TWO_WAY);
        connect(tgradient.getNegative(RankUpdatePort.class), gradient.getPositive(RankUpdatePort.class), Channel.TWO_WAY);

    }

    /**
     * Boot Up the gradient service.
     */
    private void startGradient() {

        log.debug("Starting Gradient component.");
        trigger(new OverlayViewUpdate.Indication<PeerDescriptor>(new PeerDescriptor(self.getAddress())), tgradient.getNegative(ViewUpdatePort.class));
    }

    private void connectCroupier() {

        log.info("connecting croupier components...");
        Identifier croupierOverlay = new IntIdentifier(getCroupierServiceInt(getCroupierServiceByteArray()));

        croupier = create(CroupierComp.class, new CroupierComp.CroupierInit(croupierOverlay, self.getAddress()));
        connect(croupier.getNegative(Timer.class), timer, Channel.TWO_WAY);
        connect(croupier.getNegative(Network.class), network, new OverlaySelector(croupierOverlay, true), Channel.TWO_WAY);
        connect(croupier.getPositive(CroupierPort.class), routing.getNegative(CroupierPort.class), Channel.TWO_WAY);
        connect(croupier.getNegative(AddressUpdatePort.class), selfAddressUpdatePort, Channel.TWO_WAY);

        //TODO Alex - fix this later - need local port, WARN
        subscribe(handleCroupierDisconnect, croupier.getPositive(CroupierControlPort.class));
        log.debug("expecting start croupier next");
    }

    private int getCroupierServiceInt(byte[] serviceArray) {
        return Ints.fromByteArray(serviceArray);
    }

    private Handler handleCroupierDisconnect = new Handler<CroupierDisconnected>() {

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
    ClassMatchedHandler addEntrySimulatorHandler
            = new ClassMatchedHandler<AddIndexEntryP2pSimulated, BasicContentMsg<KAddress, KHeader<KAddress>, AddIndexEntryP2pSimulated>>() {
                @Override
                public void handle(AddIndexEntryP2pSimulated request, BasicContentMsg<KAddress, KHeader<KAddress>, AddIndexEntryP2pSimulated> event) {
                    log.debug("{}: Add Entry Received for Node:", self.getId());
                    trigger(new SimulationEventsPort.AddIndexSimulated(request.getIndexEntry()), search.getNegative(SimulationEventsPort.class));
                }
            };

    KAddress currentSimAddress = null;

    ClassMatchedHandler searchSimulatorHandler
            = new ClassMatchedHandler<SearchP2pSimulated.Request, BasicContentMsg<KAddress, KHeader<KAddress>, SearchP2pSimulated.Request>>() {

                @Override
                public void handle(SearchP2pSimulated.Request request, BasicContentMsg<KAddress, KHeader<KAddress>, SearchP2pSimulated.Request> event) {

                    currentSimAddress = event.getSource();
                    log.debug("Search Event Received : {}", request.getSearchPattern());
                    trigger(new SimulationEventsPort.SearchSimulated.Request(request.getSearchPattern(), request.getSearchTimeout(), request.getFanoutParameter()), search.getNegative(SimulationEventsPort.class));
                }
            };

    /**
     * Handler for the Response from the Search Component regarding the
     * responses and the partitions hit for the request.
     */
    Handler<SimulationEventsPort.SearchSimulated.Response> searchSimulatedResponseHandler = new Handler<SimulationEventsPort.SearchSimulated.Response>() {
        @Override
        public void handle(SimulationEventsPort.SearchSimulated.Response event) {

            log.debug("{}: Received the search simulated response from the child component", self.getId());
            if (currentSimAddress != null) {
                log.debug("Responses: {}, Partitions Hit: {}", event.getResponses(), event.getPartitionHit());
                SearchP2pSimulated.Response response = new SearchP2pSimulated.Response(event.getResponses(), event.getPartitionHit());
                trigger(CommonHelper.getDecoratedContentMessage(self.getAddress(), currentSimAddress, Transport.UDP, response), network);
            }
        }
    };

}
