/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.ms.search;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import org.javatuples.Pair;
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
import se.sics.ktoolbox.croupier.CroupierPort;
import se.sics.ktoolbox.election.ElectionFollower;
import se.sics.ktoolbox.election.ElectionInit;
import se.sics.ktoolbox.election.ElectionLeader;
import se.sics.ktoolbox.election.api.ports.LeaderElectionPort;
import se.sics.ktoolbox.gradient.GradientPort;
import se.sics.ktoolbox.overlaymngr.OverlayMngrPort;
import se.sics.ktoolbox.overlaymngr.events.OMngrTGradient;
import se.sics.ktoolbox.util.address.AddressUpdatePort;
import se.sics.ktoolbox.util.config.impl.SystemKCWrapper;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.basic.IntIdentifier;
import se.sics.ktoolbox.util.identifier.OverlayIdHelper;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.ports.One2NChannel;
import se.sics.ktoolbox.util.overlays.EventOverlayIdExtractor;
import se.sics.ktoolbox.util.overlays.view.OverlayViewUpdatePort;
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

    //*****************************CONNECTIONS**********************************
    //*****************************CONNECT TO***********************************
    //provided external ports
    private final Positive<OverlayMngrPort> omngrPort = requires(OverlayMngrPort.class);
    //providing external ports
    private final Negative<UiPort> uiPort = provides(UiPort.class);
    //**************************DO NOT CONNECT TO*******************************
    //provided external ports
    private final ExtPort extPort;
    private final One2NChannel<CroupierPort> croupierEnd;
    private final One2NChannel<GradientPort> gradientEnd;
    private final One2NChannel<OverlayViewUpdatePort> viewUpdateEnd;
    //****************************CONFIGURATION*********************************
    private final SystemKCWrapper systemConfig;
    private final SearchPeerKCWrapper spConfig;
    private final KeyPair appKey;
    //hack - remove later
    private final GradientConfiguration gradientConfiguration;
    private final SearchConfiguration searchConfiguration;
    //*******************************SELF***************************************
    //this should be removed... not really necessary;
    private KAddress selfAdr;
    //hack - remove later
    private final ApplicationSelf self;
    //****************************CLEANUP***************************************
    private Pair<Component, Channel[]> electionLeader, electionFollower;
    private Pair<Component, Channel[]> routing;
    private Pair<Component, Channel[]> search;
    //**************************************************************************

    public SearchPeerComp(Init init) {
        systemConfig = new SystemKCWrapper(config());
        spConfig = new SearchPeerKCWrapper();
        appKey = generateKeys();
        selfAdr = init.selfAdr;

        logPrefix = " ";
        LOG.info("{}initiating...", logPrefix);

        extPort = init.extPort;
        croupierEnd = One2NChannel.getChannel(extPort.croupierPort, new EventOverlayIdExtractor());
        gradientEnd = One2NChannel.getChannel(extPort.gradientPort, new EventOverlayIdExtractor());
        viewUpdateEnd = One2NChannel.getChannel(extPort.viewUpdatePort, new EventOverlayIdExtractor());

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

    //******************************CONNECT*************************************
    private void connectElection() {
        LOG.info("{}connecting election components", logPrefix);

        LEContainerComparator containerComparator = new LEContainerComparator(new SimpleLCPViewComparator(), new ComparatorCollection.AddressComparator());
        ApplicationRuleSet.SweepLCRuleSet leaderComponentRuleSet = new ApplicationRuleSet.SweepLCRuleSet(containerComparator);
        ApplicationRuleSet.SweepCohortsRuleSet cohortsRuleSet = new ApplicationRuleSet.SweepCohortsRuleSet(containerComparator);

        Component leaderComp = create(ElectionLeader.class, new ElectionInit<ElectionLeader>(
                selfAdr, new PeerDescriptor(selfAdr), appKey.getPublic(), appKey.getPrivate(),
                new SimpleLCPViewComparator(), leaderComponentRuleSet, cohortsRuleSet));
        Component followerComp = create(ElectionFollower.class, new ElectionInit<ElectionFollower>(
                selfAdr, new PeerDescriptor(selfAdr), appKey.getPublic(), appKey.getPrivate(),
                new SimpleLCPViewComparator(), leaderComponentRuleSet, cohortsRuleSet));

        Channel[] leaderChannels = new Channel[3];
        leaderChannels[0] = connect(leaderComp.getNegative(Network.class), extPort.networkPort, Channel.TWO_WAY);
        leaderChannels[1] = connect(leaderComp.getNegative(Timer.class), extPort.timerPort, Channel.TWO_WAY);
        leaderChannels[2] = connect(leaderComp.getNegative(AddressUpdatePort.class), extPort.addressUpdatePort, Channel.TWO_WAY);
        gradientEnd.addChannel(spConfig.tgradientId, leaderComp.getNegative(GradientPort.class));

        Channel[] followerChannels = new Channel[3];
        followerChannels[0] = connect(followerComp.getNegative(Network.class), extPort.networkPort, Channel.TWO_WAY);
        followerChannels[1] = connect(followerComp.getNegative(Timer.class), extPort.timerPort, Channel.TWO_WAY);
        followerChannels[2] = connect(followerComp.getNegative(AddressUpdatePort.class), extPort.addressUpdatePort, Channel.TWO_WAY);
        gradientEnd.addChannel(spConfig.tgradientId, followerComp.getNegative(GradientPort.class));

        electionLeader = Pair.with(leaderComp, leaderChannels);
        electionFollower = Pair.with(followerComp, followerChannels);
    }

    //TODO Alex - revisit Routing and NPAwareSearch when time - fishy
    private void connectRouting() {
        Identifier croupierId = OverlayIdHelper.changeOverlayType((IntIdentifier) spConfig.tgradientId, OverlayIdHelper.Type.CROUPIER);
        Component routingComp = create(Routing.class, new RoutingInit(systemConfig.seed, self, gradientConfiguration));
        Channel[] routingChannels = new Channel[4];
        routingChannels[0] = connect(routingComp.getNegative(Timer.class), extPort.timerPort, Channel.TWO_WAY);
        routingChannels[1] = connect(routingComp.getNegative(Network.class), extPort.networkPort, Channel.TWO_WAY);
        routingChannels[2] = connect(routingComp.getNegative(LeaderElectionPort.class), electionLeader.getValue0().getPositive(LeaderElectionPort.class), Channel.TWO_WAY);
        routingChannels[3] = connect(routingComp.getNegative(LeaderElectionPort.class), electionFollower.getValue0().getPositive(LeaderElectionPort.class), Channel.TWO_WAY);
        croupierEnd.addChannel(croupierId, routingComp.getNegative(CroupierPort.class));
        gradientEnd.addChannel(spConfig.tgradientId, routingComp.getNegative(GradientPort.class));
        //LeaderStatusPort, SelfChangedPort - provided by search
        //GradientRoutingPort - required by search
        //what is GradientViewChangePort used for?
        routing = Pair.with(routingComp, routingChannels);
    }

    private void connectSearch() {
        Component searchComp = create(NPAwareSearch.class, new SearchInit(spConfig.tgradientId, systemConfig.seed,
                self, searchConfiguration, appKey.getPublic(), appKey.getPrivate()));
        Channel[] searchChannels = new Channel[9];
        //requires
        searchChannels[0] = connect(searchComp.getNegative(Timer.class), extPort.timerPort, Channel.TWO_WAY);
        searchChannels[1] = connect(searchComp.getNegative(Network.class), extPort.networkPort, Channel.TWO_WAY);
        searchChannels[2] = connect(searchComp.getNegative(AddressUpdatePort.class), extPort.addressUpdatePort, Channel.TWO_WAY);
        searchChannels[3] = connect(searchComp.getNegative(LeaderElectionPort.class), electionLeader.getValue0().getPositive(LeaderElectionPort.class), Channel.TWO_WAY);
        searchChannels[4] = connect(searchComp.getNegative(LeaderElectionPort.class), electionFollower.getValue0().getPositive(LeaderElectionPort.class), Channel.TWO_WAY);
        searchChannels[5] = connect(searchComp.getNegative(GradientRoutingPort.class), routing.getValue0().getPositive(GradientRoutingPort.class), Channel.TWO_WAY);
        gradientEnd.addChannel(spConfig.tgradientId, searchComp.getNegative(GradientPort.class));

        //provide
        searchChannels[6] = connect(searchComp.getPositive(LeaderStatusPort.class), routing.getValue0().getNegative(LeaderStatusPort.class), Channel.TWO_WAY);
        searchChannels[7] = connect(searchComp.getPositive(SelfChangedPort.class), routing.getValue0().getNegative(SelfChangedPort.class), Channel.TWO_WAY);
        //searchChannels[8] = connect(searchComp.getPositive(SelfChangedPort.class), routing.getValue0().getNegative(SelfChangedPort.class), Channel.TWO_WAY);
        searchChannels[8] = connect(searchComp.getPositive(UiPort.class), uiPort, Channel.TWO_WAY);
        viewUpdateEnd.addChannel(spConfig.tgradientId, searchComp.getPositive(OverlayViewUpdatePort.class));
        //SimulationEventPorts - what is this?
        //LocalAggregatorPort - skipped for the moment
        //PALPort/PAGPort - was this working at any point
        search = Pair.with(searchComp, searchChannels);
    }
    
    private void createOverlays() {
        LOG.info("{}setting up the tgradient overlay:{}", new Object[]{logPrefix, spConfig.tgradientId});
        OMngrTGradient.ConnectRequest req = new OMngrTGradient.ConnectRequest(spConfig.tgradientId,
                new SimpleUtilityComparator(), new SweepGradientFilter());
        trigger(req, omngrPort);
    }
    //*****************************CONTROL**************************************
    Handler handleStart = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            LOG.info("{}starting...", logPrefix);
            connectElection();
            connectRouting();
            connectSearch();
            createOverlays();
        }
    };

    //**************************CREATE OVERLAYS*********************************
    Handler handleOverlayResponse = new Handler<OMngrTGradient.ConnectResponse>() {
        @Override
        public void handle(OMngrTGradient.ConnectResponse event) {
            LOG.info("{}overlays created", logPrefix);
            trigger(Start.event, electionLeader.getValue0().control());
            trigger(Start.event, electionFollower.getValue0().control());
            trigger(Start.event, routing.getValue0().control());
            trigger(Start.event, search.getValue0().control());
        }
    };

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

        public final Positive<Timer> timerPort;
        //network ports
        public final Positive<Network> networkPort;
        public final Positive<AddressUpdatePort> addressUpdatePort;
        //overlay ports
        public final Positive<CroupierPort> croupierPort;
        public final Positive<GradientPort> gradientPort;
        public final Negative<OverlayViewUpdatePort> viewUpdatePort;

        public ExtPort(Positive<Timer> timerPort, Positive<Network> networkPort,
                Positive<AddressUpdatePort> addressUpdatePort,
                Positive<CroupierPort> croupierPort, Positive<GradientPort> gradientPort,
                Negative<OverlayViewUpdatePort> viewUpdatePort) {
            this.timerPort = timerPort;
            this.networkPort = networkPort;
            this.addressUpdatePort = addressUpdatePort;
            this.croupierPort = croupierPort;
            this.gradientPort = gradientPort;
            this.viewUpdatePort = viewUpdatePort;
        }
    }
}
