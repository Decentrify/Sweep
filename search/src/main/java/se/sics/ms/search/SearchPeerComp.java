/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.ms.search;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.Channel;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.Timer;
import se.sics.ktoolbox.chunkmanager.ChunkManagerComp;
import se.sics.ktoolbox.chunkmanager.ChunkManagerConfig;
import se.sics.ktoolbox.croupier.CroupierPort;
import se.sics.ktoolbox.election.ElectionFollower;
import se.sics.ktoolbox.election.ElectionInit;
import se.sics.ktoolbox.election.ElectionLeader;
import se.sics.ktoolbox.election.api.ports.LeaderElectionPort;
import se.sics.ktoolbox.gradient.GradientPort;
import se.sics.ktoolbox.overlaymngr.OverlayMngrPort;
import se.sics.ktoolbox.overlaymngr.events.OMngrTGradient;
import se.sics.ktoolbox.overlaymngr.util.EventOverlaySelector;
import se.sics.ktoolbox.util.address.AddressUpdatePort;
import se.sics.ktoolbox.util.config.impl.SystemKCWrapper;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.update.view.ViewUpdatePort;
import se.sics.ms.common.ApplicationSelf;
import se.sics.ms.gradient.gradient.Routing;
import se.sics.ms.gradient.gradient.RoutingInit;
import se.sics.ms.gradient.gradient.SweepGradientFilter;
import se.sics.ms.gradient.misc.SimpleUtilityComparator;
import se.sics.ms.gradient.ports.GradientRoutingPort;
import se.sics.ms.gradient.ports.LeaderStatusPort;
import se.sics.ms.gvod.config.GradientConfiguration;
import se.sics.ms.gvod.config.SearchConfiguration;
import se.sics.ms.ports.SelfChangedPort;
import se.sics.ms.ports.UiPort;
import se.sics.ms.types.PeerDescriptor;
import se.sics.ms.util.ApplicationRuleSet;
import se.sics.ms.util.ComparatorCollection;
import se.sics.ms.util.LEContainerComparator;
import se.sics.ms.util.SimpleLCPViewComparator;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class SearchPeerComp extends ComponentDefinition {
    //TODO Alex - connect aggregator

    private final static Logger LOG = LoggerFactory.getLogger(SearchPeerComp.class);
    private String logPrefix = "";

    private SystemKCWrapper systemConfig;
    private SearchPeerKCWrapper spConfig;
    private KeyPair appKey;
    
    private final Positive omngrPort = requires(OverlayMngrPort.class);
    //these ExtPort should not change
    private final ExtPort extPort;
    private Component electionLeader, electionFollower;
    private Component chunkMngrComp;
    private Component routingComp, searchComp;

    //this should be removed... not really necessary;
    private KAddress selfAdr;
    //hack - remove later
    private final ApplicationSelf self;
    private final GradientConfiguration gradientConfiguration;
    private final SearchConfiguration searchConfiguration;

    public SearchPeerComp(Init init) {
        systemConfig = new SystemKCWrapper(config());
        spConfig = new SearchPeerKCWrapper();
        appKey = generateKeys();
        selfAdr = init.selfAdr;

        logPrefix = " ";
        LOG.info("{}initiating...", logPrefix);

        extPort = init.extPort;

        //hack
        self = new ApplicationSelf(selfAdr);
        gradientConfiguration = init.gradientConfiguration;
        searchConfiguration = init.searchConfiguration;

        subscribe(handleStart, control);
        subscribe(handleOverlayResponse, omngrPort);
    }

    private KeyPair generateKeys() {
        try {
            KeyPairGenerator keyGen;
            keyGen = KeyPairGenerator.getInstance("RSA");
            keyGen.initialize(1024);
            return keyGen.generateKeyPair();
        } catch (NoSuchAlgorithmException ex) {
            throw new RuntimeException("can't generate keys");
        }
    }
    //*****************************CONTROL**************************************
    Handler handleStart = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            LOG.info("{}starting...", logPrefix);
            createOverlays();
        }
    };

    //**************************CREATE OVERLAYS*********************************
    private void createOverlays() {
        LOG.info("{}setting up the overlays", logPrefix);
        OMngrTGradient.ConnectRequestBuilder req = new OMngrTGradient.ConnectRequestBuilder();
        req.setIdentifiers(spConfig.croupierId, spConfig.gradientId, spConfig.tgradientId);
        req.setupGradient(new SimpleUtilityComparator(), new SweepGradientFilter());
        trigger(req.build(), omngrPort);
    }

    Handler handleOverlayResponse = new Handler<OMngrTGradient.ConnectResponse>() {
        @Override
        public void handle(OMngrTGradient.ConnectResponse event) {
            LOG.info("{}overlays created", logPrefix);
            connectElection();
            connectChunkManager();
            connectRouting();
            connectSearch();
            trigger(Start.event, electionLeader.control());
            trigger(Start.event, electionFollower.control());
            trigger(Start.event, chunkMngrComp.control());
            trigger(Start.event, routingComp.control());
            trigger(Start.event, searchComp.control());
        }
    };

    //****************************CONNECT REST**********************************
    /**
     * Assumptions - network, timer, overlays are created and started
     *
     * @param electionConfig
     * @param seed
     */
    private void connectElection() {
        LOG.info("{}connecting election components", logPrefix);

        LEContainerComparator containerComparator = new LEContainerComparator(new SimpleLCPViewComparator(), new ComparatorCollection.AddressComparator());
        ApplicationRuleSet.SweepLCRuleSet leaderComponentRuleSet = new ApplicationRuleSet.SweepLCRuleSet(containerComparator);
        ApplicationRuleSet.SweepCohortsRuleSet cohortsRuleSet = new ApplicationRuleSet.SweepCohortsRuleSet(containerComparator);

        electionLeader = create(ElectionLeader.class, new ElectionInit<ElectionLeader>(
                selfAdr, new PeerDescriptor(selfAdr), appKey.getPublic(), appKey.getPrivate(),
                new SimpleLCPViewComparator(), leaderComponentRuleSet, cohortsRuleSet));

        electionFollower = create(ElectionFollower.class, new ElectionInit<ElectionFollower>(
                selfAdr, new PeerDescriptor(selfAdr), appKey.getPublic(), appKey.getPrivate(),
                new SimpleLCPViewComparator(), leaderComponentRuleSet, cohortsRuleSet));

        Channel[] electionChannels = new Channel[8];
        // Election leader connections.
        electionChannels[0] = connect(electionLeader.getNegative(Network.class), extPort.networkPort, Channel.TWO_WAY);
        electionChannels[1] = connect(electionLeader.getNegative(Timer.class), extPort.timerPort, Channel.TWO_WAY);
        electionChannels[2] = connect(electionLeader.getNegative(AddressUpdatePort.class), extPort.addressUpdatePort, Channel.TWO_WAY);
        electionChannels[3] = connect(electionLeader.getNegative(GradientPort.class), extPort.gradientPort,
                new EventOverlaySelector(spConfig.tgradientId, true), Channel.TWO_WAY);

        // Election follower connections.
        electionChannels[4] = connect(electionFollower.getNegative(Network.class), extPort.networkPort, Channel.TWO_WAY);
        electionChannels[5] = connect(electionFollower.getNegative(Timer.class), extPort.timerPort, Channel.TWO_WAY);
        electionChannels[6] = connect(electionFollower.getNegative(AddressUpdatePort.class), extPort.addressUpdatePort, Channel.TWO_WAY);
        electionChannels[7] = connect(electionFollower.getNegative(GradientPort.class), extPort.gradientPort,
                new EventOverlaySelector(spConfig.tgradientId, true), Channel.TWO_WAY);
    }

    /**
     * Assumptions - network, timer are created and started
     */
    private void connectChunkManager() {
        //TODO Alex - remove hardocoded values;
        chunkMngrComp = create(ChunkManagerComp.class, new ChunkManagerComp.CMInit(new ChunkManagerConfig(30000, 1000), selfAdr));
        Channel[] chunkMngrChannels = new Channel[2];
        chunkMngrChannels[0] = connect(chunkMngrComp.getNegative(Network.class), extPort.networkPort, Channel.TWO_WAY);
        chunkMngrChannels[1] = connect(chunkMngrComp.getNegative(Timer.class), extPort.timerPort, Channel.TWO_WAY);
    }

    //TODO Alex - revisit Routing and NPAwareSearch when time - fishy
    private void connectRouting() {
        routingComp = create(Routing.class, new RoutingInit(systemConfig.seed, self, gradientConfiguration));
        Channel[] routingChannels = new Channel[6];
        routingChannels[0] = connect(routingComp.getNegative(Timer.class), extPort.timerPort, Channel.TWO_WAY);
        routingChannels[1] = connect(routingComp.getNegative(Network.class), chunkMngrComp.getPositive(Network.class), Channel.TWO_WAY);
        routingChannels[2] = connect(routingComp.getNegative(CroupierPort.class), extPort.croupierPort,
                new EventOverlaySelector(spConfig.croupierId, true), Channel.TWO_WAY);
        routingChannels[3] = connect(routingComp.getNegative(GradientPort.class), extPort.gradientPort,
                new EventOverlaySelector(spConfig.tgradientId, true), Channel.TWO_WAY);
        routingChannels[4] = connect(routingComp.getNegative(LeaderElectionPort.class), electionLeader.getPositive(LeaderElectionPort.class), Channel.TWO_WAY);
        routingChannels[5] = connect(routingComp.getNegative(LeaderElectionPort.class), electionFollower.getPositive(LeaderElectionPort.class), Channel.TWO_WAY);
        //LeaderStatusPort, SelfChangedPort - provided by search
        //GradientRoutingPort - required by search
        //what is GradientViewChangePort used for?
    }

    private void connectSearch() {
        searchComp = create(NPAwareSearch.class, new SearchInit(systemConfig.seed, self, searchConfiguration, appKey.getPublic(), appKey.getPrivate()));
        Channel[] searchChannels = new Channel[12];
        //requires
        searchChannels[0] = connect(searchComp.getNegative(Timer.class), extPort.timerPort, Channel.TWO_WAY);
        searchChannels[1] = connect(searchComp.getNegative(Network.class), chunkMngrComp.getPositive(Network.class), Channel.TWO_WAY);
        searchChannels[2] = connect(searchComp.getNegative(AddressUpdatePort.class), extPort.addressUpdatePort, Channel.TWO_WAY);
        searchChannels[3] = connect(searchComp.getNegative(GradientPort.class), extPort.gradientPort,
                new EventOverlaySelector(spConfig.tgradientId, true), Channel.TWO_WAY);
        searchChannels[4] = connect(searchComp.getNegative(LeaderElectionPort.class), electionLeader.getPositive(LeaderElectionPort.class), Channel.TWO_WAY);
        searchChannels[5] = connect(searchComp.getNegative(LeaderElectionPort.class), electionFollower.getPositive(LeaderElectionPort.class), Channel.TWO_WAY);
        searchChannels[6] = connect(searchComp.getNegative(GradientRoutingPort.class), routingComp.getPositive(GradientRoutingPort.class), Channel.TWO_WAY);
        //provide
        searchChannels[7] = connect(searchComp.getPositive(LeaderStatusPort.class), routingComp.getNegative(LeaderStatusPort.class), Channel.TWO_WAY);
        searchChannels[8] = connect(searchComp.getPositive(SelfChangedPort.class), routingComp.getNegative(SelfChangedPort.class), Channel.TWO_WAY);
        searchChannels[9] = connect(searchComp.getPositive(SelfChangedPort.class), routingComp.getNegative(SelfChangedPort.class), Channel.TWO_WAY);
        searchChannels[10] = connect(searchComp.getPositive(ViewUpdatePort.class), extPort.viewUpdatePort,
                new EventOverlaySelector(spConfig.tgradientId, true), Channel.TWO_WAY);
        searchChannels[11] = connect(searchComp.getPositive(UiPort.class), extPort.uiPort, Channel.TWO_WAY);
        //SimulationEventPorts - what is this?
        //LocalAggregatorPort - skipped for the moment
        //PALPort/PAGPort - was this working at any point
    }
    //**************************************************************************
    public static class Init extends se.sics.kompics.Init<SearchPeerComp> {

        public final KAddress selfAdr;
        public final ExtPort extPort;

        //TODO Alex - remove later - for the moment need for run/compile
        public final GradientConfiguration gradientConfiguration;
        public final SearchConfiguration searchConfiguration;

        public Init(KAddress selfAdr, ExtPort extPort,
                GradientConfiguration gradientConfiguration, SearchConfiguration searchConfiguration) {
            this.selfAdr = selfAdr;
            this.extPort = extPort;

            //hack
            this.gradientConfiguration = gradientConfiguration;
            this.searchConfiguration = searchConfiguration;
        }
    }

    //**************************************************************************
    public static class ExtPort {

        public final Positive timerPort;
        //network ports
        public final Positive addressUpdatePort;
        public final Positive networkPort;
        //overlay ports
        public final Positive croupierPort;
        public final Positive gradientPort;
        public final Negative viewUpdatePort;
        //app specific port
        public final Negative uiPort;

        public ExtPort(Positive timerPort, Positive networkPort, Positive addressUpdatePort,
                Positive croupierPort, Positive gradientPort, Negative viewUpdatePort,
                Negative uiPort) {
            this.timerPort = timerPort;
            this.networkPort = networkPort;
            this.addressUpdatePort = addressUpdatePort;
            this.croupierPort = croupierPort;
            this.gradientPort = gradientPort;
            this.viewUpdatePort = viewUpdatePort;
            this.uiPort = uiPort;
        }
    }
}
