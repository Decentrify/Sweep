package se.sics.ms.gradient;

import se.sics.gvod.common.Self;
import se.sics.gvod.common.VodDescriptor;
import se.sics.gvod.config.GradientConfiguration;
import se.sics.gvod.croupier.PeerSamplePort;
import se.sics.gvod.croupier.events.CroupierSample;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.VodNetwork;
import se.sics.gvod.timer.*;
import se.sics.gvod.timer.Timer;
import se.sics.gvod.timer.UUID;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.ms.configuration.MsConfig;
import se.sics.ms.gradient.BroadcastGradientPartnersPort.GradientPartners;
import se.sics.ms.gradient.LeaderStatusPort.LeaderStatus;
import se.sics.ms.gradient.LeaderStatusPort.NodeCrashEvent;
import se.sics.ms.timeout.IndividualTimeout;
import se.sics.peersearch.messages.AddIndexEntryMessage;
import se.sics.peersearch.messages.GradientShuffleMessage;
import se.sics.peersearch.messages.LeaderLookupMessage;
import se.sics.peersearch.types.IndexEntry;

import java.util.*;


/**
 * Component creating a gradient network from Croupier samples according to a
 * preference function.
 */
public final class Gradient extends ComponentDefinition {

    Positive<PeerSamplePort> croupierSamplePort = positive(PeerSamplePort.class);
    Positive<VodNetwork> networkPort = positive(VodNetwork.class);
    Positive<Timer> timerPort = positive(Timer.class);
    Positive<BroadcastGradientPartnersPort> broadcastGradientPartnersPort = positive(BroadcastGradientPartnersPort.class);
    Negative<LeaderStatusPort> leaderStatusPort = negative(LeaderStatusPort.class);
    Negative<LeaderRequestPort> leaderRequestPort = negative(LeaderRequestPort.class);

    private Self self;
    private GradientConfiguration config;
    private Random random;
    private GradientView gradientView;
    private Map<UUID, VodAddress> outstandingShuffles;
    private boolean leader;

    /**
     * Timeout to periodically issue exchanges.
     */
    public class GradientRound extends IndividualTimeout {

        public GradientRound(SchedulePeriodicTimeout request, int id) {
            super(request, id);
        }
    }

    public class ShuffleRequestTimeout extends IndividualTimeout {

        public ShuffleRequestTimeout(ScheduleTimeout request, int id) {
            super(request, id);
        }
    }

    public Gradient() {
        subscribe(handleInit, control);
        subscribe(handleRound, timerPort);
        subscribe(handleRequestTimeout, timerPort);
        subscribe(handleCroupierSample, croupierSamplePort);
        subscribe(handleShuffleResponse, networkPort);
        subscribe(handleShuffleRequest, networkPort);
        subscribe(handleLeaderLookupRequest, networkPort);
        subscribe(handleLeaderLookupResponse, networkPort);
        subscribe(handleLeaderStatus, leaderStatusPort);
        subscribe(handleNodeCrash, leaderStatusPort);
        subscribe(handleAddIndexEntryRequest, leaderRequestPort);
    }
    /**
     * Initialize the state of the component.
     */
    final Handler<GradientInit> handleInit = new Handler<GradientInit>() {
        @Override
        public void handle(GradientInit init) {
            self = init.getSelf();
            config = init.getConfiguration();
            outstandingShuffles = Collections.synchronizedMap(new HashMap<UUID, VodAddress>());
            random = new Random(init.getConfiguration().getSeed());
            gradientView = new GradientView(self, config.getViewSize(),
                    config.getConvergenceTest());
            leader = false;

            SchedulePeriodicTimeout rst = new SchedulePeriodicTimeout(config.getShufflePeriod(), config.getShufflePeriod());
            rst.setTimeoutEvent(new GradientRound(rst, self.getId()));
            trigger(rst, timerPort);
        }
    };
    /**
     * Initiate a identifier exchange every round.
     */
    final Handler<GradientRound> handleRound = new Handler<GradientRound>() {
        @Override
        public void handle(GradientRound event) {
            if (!gradientView.isEmpty()) {
                initiateShuffle(gradientView.selectPeerToShuffleWith());
            }

//            System.out.println("View of node " + self.getId() + ": " + gradientView.toString() + " converged? " + gradientView.isConverged());
        }
    };
    /**
     * Initiate a exchange with a random node of each Croupier sample to speed
     * up convergence and prevent partitioning.
     */
    final Handler<CroupierSample> handleCroupierSample = new Handler<CroupierSample>() {
        @Override
        public void handle(CroupierSample event) {
            List<VodDescriptor> sample = event.getNodes();

            // Remove all samples from other partitions
            Iterator<VodDescriptor> iterator = sample.iterator();
            while (iterator.hasNext()) {
                // TODO Number of partition from proper config file
                if(iterator.next().getVodAddress().getId() % MsConfig.SEARCH_NUM_PARTITIONS != self.getId() % MsConfig.SEARCH_NUM_PARTITIONS)  {
                    iterator.remove();
                }
            }

            if (sample.size() > 0) {
                int n = random.nextInt(sample.size());
                initiateShuffle(sample.get(n).getVodAddress());
            }
        }
    };
    /**
     * Answer a {@link GradientShuffleMessage.Request} with the nodes from the view preferred by
     * the inquirer.
     */
    final Handler<GradientShuffleMessage.Request> handleShuffleRequest = new Handler<GradientShuffleMessage.Request>() {
        @Override
        public void handle(GradientShuffleMessage.Request event) {
//            System.out.println(self.getAddress().toString() + " got ShuffleRequest from " + event.getVodSource().toString());

            VodAddress exchangePartner = event.getVodSource();
            Collection<VodAddress> exchange = gradientView.getExchangeNodes(exchangePartner,
                    config.getShuffleLength());

            VodAddress[] exchangeNodes = exchange.toArray(new VodAddress[exchange.size()]);

//            StringBuilder builder = new StringBuilder();
//            builder.append(self.getAddress().toString() + " sending ShuffleResponse to " + exchangePartner.toString() + "\n");
//            builder.append("Content: \n");
//            for (VodAddress a : exchangeNodes) {
//                builder.append(a.toString() + "\n");
//            }
//            System.out.println(builder.toString());

            GradientShuffleMessage.Response rResponse = new GradientShuffleMessage.Response(self.getAddress(), exchangePartner, event.getTimeoutId(), exchangeNodes);
            trigger(rResponse, networkPort);

            gradientView.merge(event.getAddresses());
            broadcastView();
        }
    };
    /**
     * Merge the entries from the response to the view.
     */
    final Handler<GradientShuffleMessage.Response> handleShuffleResponse = new Handler<GradientShuffleMessage.Response>() {
        @Override
        public void handle(GradientShuffleMessage.Response event) {
//            System.out.println(self.getAddress().toString() + " got ShuffleResponse from " + event.getVodSource().toString());

            // cancel shuffle timeout
            UUID shuffleId = (UUID) event.getTimeoutId();
            if (outstandingShuffles.containsKey(shuffleId)) {
                outstandingShuffles.remove(shuffleId);
                CancelTimeout ct = new CancelTimeout(shuffleId);
                trigger(ct, timerPort);
            }

            gradientView.merge(event.getAddresses());
            broadcastView();
        }
    };
    /**
     * This handler listens to updates regarding the leader status
     */
    final Handler<LeaderStatus> handleLeaderStatus = new Handler<LeaderStatus>() {
        @Override
        public void handle(LeaderStatus event) {
            leader = event.isLeader();
        }
    };
    /**
     * Updates gradient's view by removing crashed nodes from it, eg. old leaders
     */
    final Handler<NodeCrashEvent> handleNodeCrash = new Handler<NodeCrashEvent>() {
        @Override
        public void handle(NodeCrashEvent event) {
            gradientView.remove(event.getDeadNode());
        }
    };
    /**
     * Remove a node from the view if it didn't respond to a request.
     */
    final Handler<ShuffleRequestTimeout> handleRequestTimeout = new Handler<ShuffleRequestTimeout>() {
        @Override
        public void handle(ShuffleRequestTimeout event) {
            UUID rTimeoutId = (UUID) event.getTimeoutId();
            VodAddress deadNode = outstandingShuffles.remove(rTimeoutId);

            if (deadNode != null) {
                gradientView.remove(deadNode);
            }
        }
    };
    // TODO This is a very fragile routing implementation and only for testing purposes, it might not even terminate
    private IndexEntry entryToAdd;
    private boolean leaderFound;
    final Handler<LeaderRequestPort.AddIndexEntryRequest> handleAddIndexEntryRequest = new Handler<LeaderRequestPort.AddIndexEntryRequest>() {
        @Override
        public void handle(LeaderRequestPort.AddIndexEntryRequest event) {
            leaderFound = false;
            entryToAdd = event.getEntry();

            ArrayList<VodAddress> higherNodes = gradientView.getHigherNodes();
            for (int i = 0; i < higherNodes.size() && i < LeaderLookupMessage.A; i++) {
                trigger(new LeaderLookupMessage.Request(self.getAddress(), higherNodes.get(i), event.getTimeoutId()), networkPort);
            }
        }
    };
    final Handler<LeaderLookupMessage.Request> handleLeaderLookupRequest = new Handler<LeaderLookupMessage.Request>() {
        @Override
        public void handle(LeaderLookupMessage.Request event) {
            ArrayList<VodAddress> higherNodes = gradientView.getHigherNodes();
            int limit = higherNodes.size() > LeaderLookupMessage.K ? LeaderLookupMessage.K : higherNodes.size();
            VodAddress[] addresses = new VodAddress[limit];

            for (int i = 0; i < limit; i++) {
                addresses[i] = higherNodes.get(i);
            }

            trigger(new LeaderLookupMessage.Response(self.getAddress(), event.getVodSource(), event.getTimeoutId(), leader, addresses), networkPort);
        }
    };
    final Handler<LeaderLookupMessage.Response> handleLeaderLookupResponse = new Handler<LeaderLookupMessage.Response>() {
        @Override
        public void handle(LeaderLookupMessage.Response event) {
            if (leaderFound) {
                return;
            }

            if (event.isLeader()) {
                leaderFound = true;
                trigger(new AddIndexEntryMessage.Request(self.getAddress(), event.getVodSource(), event.getTimeoutId(), entryToAdd), networkPort);
                entryToAdd = null;
            } else {
                VodAddress[] higherNodes = event.getAddresses();
                Arrays.sort(higherNodes, closeToLeader);
                for (int i = 0; i < higherNodes.length && i < LeaderLookupMessage.A; i++) {
                    trigger(new LeaderLookupMessage.Request(self.getAddress(), higherNodes[i], event.getTimeoutId()), networkPort);
                }
            }
        }
    };

    /**
     * Initiate the shuffling process for the given node.
     *
     * @param exchangePartner the address of the node to shuffle with
     */
    private void initiateShuffle(VodAddress exchangePartner) {
        Collection<VodAddress> exchange = gradientView.getExchangeNodes(exchangePartner,
                config.getShuffleLength());

        VodAddress[] exchangeNodes = exchange.toArray(new VodAddress[exchange.size()]);

        ScheduleTimeout rst = new ScheduleTimeout(config.getShufflePeriod());
        rst.setTimeoutEvent(new ShuffleRequestTimeout(rst, self.getId()));
        UUID rTimeoutId = (UUID) rst.getTimeoutEvent().getTimeoutId();

        outstandingShuffles.put(rTimeoutId, exchangePartner);

//        StringBuilder builder = new StringBuilder();
//        builder.append(self.getAddress().toString() + " sending ShuffleRequest to " + exchangePartner.toString() + "\n");
//        builder.append("Content: \n");
//        for (VodAddress a : exchangeNodes) {
//            builder.append(a.toString() + "\n");
//        }
//        System.out.println(builder.toString());

        GradientShuffleMessage.Request rRequest = new GradientShuffleMessage.Request(self.getAddress(), exchangePartner, rTimeoutId, exchangeNodes);

        trigger(rst, timerPort);
        trigger(rRequest, networkPort);
    }

    /**
     * Broadcast the current view to the listening components.
     */
    private void broadcastView() {
        if (gradientView.isChanged()) {
            trigger(new GradientPartners(gradientView.isConverged(), gradientView.getHigherNodes(),
                    gradientView.getLowerNodes()), broadcastGradientPartnersPort);
        }
    }

    // If you call this method with a list of entries, it will
    // return a single node, weighted towards the 'best' node (as defined by
    // ComparatorById) with the temperature controlling the weighting.
    // A temperature of '1.0' will be greedy and always return the best node.
    // A temperature of '0.000001' will return a random node.
    // A temperature of '0.0' will throw a divide by zero exception :)
    // Reference:
    // http://webdocs.cs.ualberta.ca/~sutton/book/2/node4.html
    private VodAddress getSoftMaxAddress(List<VodAddress> entries) {
        Collections.sort(entries, closeToLeader);

        double rnd = random.nextDouble();
        double total = 0.0d;
        double[] values = new double[entries.size()];
        int j = entries.size() + 1;
        for (int i = 0; i < entries.size(); i++) {
            // get inverse of values - lowest have highest value.
            double val = j;
            j--;
            values[i] = Math.exp(val / config.getTemperature());
            total += values[i];
        }

        for (int i = 0; i < values.length; i++) {
            if (i != 0) {
                values[i] += values[i - 1];
            }
            // normalise the probability for this entry
            double normalisedUtility = values[i] / total;
            if (normalisedUtility >= rnd) {
                return entries.get(i);
            }
        }

        return entries.get(entries.size() - 1);
    }

    private Comparator<VodAddress> closeToLeader = new Comparator<VodAddress>() {

        @Override
        public int compare(VodAddress o1, VodAddress o2) {
            assert (o1.getOverlayId() == o2.getOverlayId());

            if (o1.getId() > o2.getId()) {
                return 1;
            } else if (o1.getId() < o2.getId()) {
                return -1;
            }
            return 0;
        }
    };
}
