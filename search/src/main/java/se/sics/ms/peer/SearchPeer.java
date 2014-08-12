package se.sics.ms.peer;

import se.sics.co.FailureDetectorPort;
import se.sics.gvod.address.Address;
import se.sics.gvod.common.Self;
import se.sics.ms.types.SearchDescriptor;
import se.sics.gvod.config.*;
import se.sics.gvod.croupier.Croupier;
import se.sics.gvod.croupier.CroupierPort;
import se.sics.gvod.croupier.PeerSamplePort;
import se.sics.gvod.croupier.events.CroupierInit;
import se.sics.gvod.croupier.events.CroupierJoin;
import se.sics.gvod.nat.traversal.NatTraverser;
import se.sics.gvod.nat.traversal.events.NatTraverserInit;
import se.sics.gvod.net.VodNetwork;
import se.sics.gvod.timer.Timer;
import se.sics.kompics.*;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Positive;
import se.sics.ms.election.ElectionFollower;
import se.sics.ms.election.ElectionInit;
import se.sics.ms.election.ElectionLeader;
import se.sics.ms.gradient.*;
import se.sics.ms.search.*;

import java.util.HashSet;
import java.util.LinkedList;

public final class SearchPeer extends ComponentDefinition {

    Positive<SimulationEventsPort> indexPort = positive(SimulationEventsPort.class);
    Positive<VodNetwork> network = positive(VodNetwork.class);
    Positive<Timer> timer = positive(Timer.class);
    Negative<UiPort> internalUiPort = negative(UiPort.class);
    Positive<UiPort> externalUiPort = positive(UiPort.class);
    Positive<FailureDetectorPort> fdPort = requires(FailureDetectorPort.class);
    private Component croupier, gradient, search, electionLeader, electionFollower, natTraversal;
    private Self self;
    private SearchConfiguration searchConfiguration;

    public SearchPeer() {
        natTraversal = create(NatTraverser.class);
        croupier = create(Croupier.class);
        gradient = create(Gradient.class);
        search = create(Search.class);
        electionLeader = create(ElectionLeader.class);
        electionFollower = create(ElectionFollower.class);

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

        connect(timer, natTraversal.getNegative(Timer.class));
        connect(timer, search.getNegative(Timer.class));
        connect(timer, croupier.getNegative(Timer.class));
        connect(timer, gradient.getNegative(Timer.class));
        connect(timer, electionLeader.getNegative(Timer.class));
        connect(timer, electionFollower.getNegative(Timer.class));

        connect(croupier.getPositive(PeerSamplePort.class),
                gradient.getNegative(PeerSamplePort.class));
        connect(indexPort, search.getNegative(SimulationEventsPort.class));
        connect(gradient.getNegative(PublicKeyPort.class),
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
        connect(gradient.getPositive(GradientRoutingPort.class),
                search.getNegative(GradientRoutingPort.class));


        connect(internalUiPort, search.getPositive(UiPort.class));

        connect(search.getNegative(FailureDetectorPort.class), fdPort);
        connect(gradient.getNegative(FailureDetectorPort.class), fdPort);
        connect(electionLeader.getNegative(FailureDetectorPort.class), fdPort);
        connect(electionFollower.getNegative(FailureDetectorPort.class), fdPort);


        subscribe(handleInit, control);
        subscribe(searchRequestHandler, externalUiPort);
        subscribe(searchResponseHandler, search.getPositive(UiPort.class));
        subscribe(addIndexEntryRequestHandler, externalUiPort);
        subscribe(addIndexEntryUiResponseHandler, search.getPositive(UiPort.class));
    }
    Handler<SearchPeerInit> handleInit = new Handler<SearchPeerInit>() {
        @Override
        public void handle(final SearchPeerInit init) {
            self = init.getSelf();
            CroupierConfiguration croupierConfiguration = init.getCroupierConfiguration();
            GradientConfiguration gradientConfiguration = init.getGradientConfiguration();
            ElectionConfiguration electionConfiguration = init.getElectionConfiguration();
            searchConfiguration = init.getSearchConfiguration();

            trigger(new ElectionInit(self, electionConfiguration), electionLeader.getControl());
            trigger(new ElectionInit(self, electionConfiguration), electionFollower.getControl());
            trigger(new GradientInit(self, gradientConfiguration), gradient.getControl());
            trigger(new CroupierInit(self, croupierConfiguration), croupier.getControl());
            trigger(new NatTraverserInit(self, new HashSet<Address>(), croupierConfiguration.getSeed(), NatTraverserConfiguration.build(),
                    HpClientConfiguration.build(),
                    RendezvousServerConfiguration.build().
                    setSessionExpirationTime(30 * 1000),
                    StunServerConfiguration.build(),
                    StunClientConfiguration.build(),
                    ParentMakerConfiguration.build(), true), natTraversal.control());

            LinkedList<SearchDescriptor> descs = new LinkedList<SearchDescriptor>();
            if (init.getBootstrappingNode() != null) {
                final SearchDescriptor descr = new SearchDescriptor(init.getBootstrappingNode());
                descs.add(descr);
            }

            trigger(new CroupierJoin(SearchDescriptor.toVodDescriptorList(descs)), croupier.getPositive(CroupierPort.class));
            trigger(new SearchInit(self, searchConfiguration), search.getControl());
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
