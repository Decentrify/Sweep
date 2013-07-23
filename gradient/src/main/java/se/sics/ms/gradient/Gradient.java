package se.sics.ms.gradient;

import java.util.*;

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
import se.sics.peersearch.messages.*;
import se.sics.ms.gradient.BroadcastGradientPartnersPort.GradientPartners;
import se.sics.ms.gradient.LeaderStatusPort.LeaderStatus;
import se.sics.ms.gradient.LeaderStatusPort.NodeCrashEvent;
import se.sics.ms.gradient.LeaderStatusPort.NodeSuggestion;
import se.sics.ms.peer.RequestTimeout;


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
    private Self self;
    private GradientConfiguration config;
    private Random random;
    private GradientView gradientView;
    private Map<UUID, VodAddress> outstandingShuffles;
    private boolean leader;

    /**
     * Timeout to periodically issue exchanges.
     */
    public class GradientRound extends Timeout {

        public GradientRound(SchedulePeriodicTimeout request) {
            super(request);
        }
    }

    public Gradient() {
        subscribe(handleInit, control);
        subscribe(handleRound, timerPort);
        subscribe(handleRequestTimeout, timerPort);
        subscribe(handleCroupierSample, croupierSamplePort);
        subscribe(handleShuffleResponse, networkPort);
        subscribe(handleShuffleRequest, networkPort);
        subscribe(handleLeaderStatus, leaderStatusPort);
        subscribe(handleNodeCrash, leaderStatusPort);
        subscribe(handeNodeSuggestion, leaderStatusPort);
    }
    /**
     * Initialize the state of the component.
     */
    Handler<GradientInit> handleInit = new Handler<GradientInit>() {
        @Override
        public void handle(GradientInit init) {
            self = init.getSelf();
            config = init.getConfiguration();
            outstandingShuffles = Collections.synchronizedMap(new HashMap<UUID, VodAddress>());
            random = new Random(init.getConfiguration().getSeed());
            gradientView = new GradientView(self, config.getViewSize(),
                    config.getConvergenceTest());
            leader = false;

            SchedulePeriodicTimeout rst = new SchedulePeriodicTimeout(
                    config.getShufflePeriod(), config.getShufflePeriod());
            rst.setTimeoutEvent(new GradientRound(rst));
            trigger(rst, timerPort);
        }
    };
    /**
     * Initiate a identifier exchange every round.
     */
    Handler<GradientRound> handleRound = new Handler<GradientRound>() {
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
    Handler<CroupierSample> handleCroupierSample = new Handler<CroupierSample>() {
        @Override
        public void handle(CroupierSample event) {
            List<VodDescriptor> sample = event.getNodes();

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
    Handler<GradientShuffleMessage.Request> handleShuffleRequest = new Handler<GradientShuffleMessage.Request>() {
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
    Handler<GradientShuffleMessage.Response> handleShuffleResponse = new Handler<GradientShuffleMessage.Response>() {
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
    Handler<LeaderStatus> handleLeaderStatus = new Handler<LeaderStatus>() {
        @Override
        public void handle(LeaderStatus event) {
            leader = event.isLeader();
        }
    };
    /**
     * Updates gradient's view by removing crashed nodes from it, eg. old leaders
     */
    Handler<NodeCrashEvent> handleNodeCrash = new Handler<NodeCrashEvent>() {
        @Override
        public void handle(NodeCrashEvent event) {
            gradientView.remove(event.getDeadNode());
        }
    };
    /**
     * A handler that takes a suggestion and adds it to the view. This will
     * prevent the node from thinking that it is a leader candidate in case it
     * only has nodes below itself even though it's not at the top of the
     * overlay topology. Even if the suggested node might not fit in perfectly
     * it can be dropped later when the node converges
     */
    Handler<NodeSuggestion> handeNodeSuggestion = new Handler<NodeSuggestion>() {
        @Override
        public void handle(NodeSuggestion event) {
            if (event.getSuggestion() != null && event.getSuggestion().getId() < self.getId()) {
                ArrayList<VodAddress> suggestionList = new ArrayList<VodAddress>();
                suggestionList.add(event.getSuggestion());
                gradientView.merge(suggestionList.toArray(new VodAddress[suggestionList.size()]));
            }
        }
    };
    /**
     * Remove a node from the view if it didn't respond to a request.
     */
    Handler<RequestTimeout> handleRequestTimeout = new Handler<RequestTimeout>() {
        @Override
        public void handle(RequestTimeout event) {
            UUID rTimeoutId = (UUID) event.getTimeoutId();
            VodAddress deadNode = outstandingShuffles.remove(rTimeoutId);

            if (deadNode != null) {
                gradientView.remove(deadNode);
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
        rst.setTimeoutEvent(new RequestTimeout(rst));
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
        Collections.sort(entries, new ClosetIdToLeader());

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

    private class ClosetIdToLeader implements Comparator<VodAddress> {

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
    }
}
