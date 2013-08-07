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
import se.sics.ms.gradient.LeaderStatusPort.LeaderStatus;
import se.sics.ms.gradient.LeaderStatusPort.NodeCrashEvent;
import se.sics.ms.timeout.IndividualTimeout;
import se.sics.peersearch.messages.*;
import se.sics.peersearch.types.IndexEntry;

import java.security.PublicKey;
import java.util.*;


/**
 * Component creating a gradient network from Croupier samples according to a
 * preference function.
 */
public final class Gradient extends ComponentDefinition {

    Positive<PeerSamplePort> croupierSamplePort = positive(PeerSamplePort.class);
    Positive<VodNetwork> networkPort = positive(VodNetwork.class);
    Positive<Timer> timerPort = positive(Timer.class);
    Positive<GradientViewChangePort> gradientViewChangePort = positive(GradientViewChangePort.class);
    Negative<LeaderStatusPort> leaderStatusPort = negative(LeaderStatusPort.class);
    Positive<PublicKeyPort> publicKeyPort = positive(PublicKeyPort.class);
    Negative<GradientRoutingPort> gradientRoutingPort = negative(GradientRoutingPort.class);

    private Self self;
    private GradientConfiguration config;
    private Random random;
    private GradientView gradientView;
    private UtilityComparator utilityComparator = new UtilityComparator();
    private Map<UUID, VodAddress> outstandingShuffles;
    private boolean leader;

    // When you partition the index you need to find new nodes
    // This is a routing table maintaining a list of pairs in each partition.
    private Map<Integer, TreeSet<VodDescriptor>> routingTable;
    Comparator<VodDescriptor> peerAgeComparator = new Comparator<VodDescriptor>() {
        @Override
        public int compare(VodDescriptor t0, VodDescriptor t1) {
            if (t0.getVodAddress().equals(t1.getVodAddress())) {
                return 0;
            } else if (t0.getAge() > t1.getAge()) {
                return 1;
            } else {
                return -1;
            }
        }
    };

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
        subscribe(handleShuffleRequestTimeout, timerPort);
        subscribe(handleCroupierSample, croupierSamplePort);
        subscribe(handleShuffleResponse, networkPort);
        subscribe(handleShuffleRequest, networkPort);
        subscribe(handleLeaderLookupRequest, networkPort);
        subscribe(handleLeaderLookupResponse, networkPort);
        subscribe(handleLeaderStatus, leaderStatusPort);
        subscribe(handleNodeCrash, leaderStatusPort);
        subscribe(handlePublicKeyBroadcast, publicKeyPort);
        subscribe(handlePublicKeyMessage, networkPort);
        subscribe(handleAddIndexEntryRequest, gradientRoutingPort);
        subscribe(handleIndexExchangeRequest, gradientRoutingPort);
        subscribe(handleReplicationPrepairCommit, gradientRoutingPort);
        subscribe(handleSearchRequest, gradientRoutingPort);
        subscribe(handleReplicationCommit, gradientRoutingPort);
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
            gradientView = new GradientView(self, config.getViewSize(), config.getConvergenceTest(), config.getConvergenceTestRounds());
            // TODO do not access constants
            routingTable = new HashMap<Integer, TreeSet<VodDescriptor>>(MsConfig.SEARCH_NUM_PARTITIONS);
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
        }
    };

    /**
     * Initiate the shuffling process for the given node.
     *
     * @param exchangePartner the address of the node to shuffle with
     */
    private void initiateShuffle(VodDescriptor exchangePartner) {
        Set<VodDescriptor> exchangeNodes = gradientView.getExchangeDescriptors(exchangePartner, config.getShuffleLength());

        ScheduleTimeout rst = new ScheduleTimeout(config.getShufflePeriod());
        rst.setTimeoutEvent(new ShuffleRequestTimeout(rst, self.getId()));
        UUID rTimeoutId = (UUID) rst.getTimeoutEvent().getTimeoutId();
        outstandingShuffles.put(rTimeoutId, exchangePartner.getVodAddress());

        GradientShuffleMessage.Request rRequest = new GradientShuffleMessage.Request(self.getAddress(), exchangePartner.getVodAddress(), rTimeoutId, exchangeNodes);

        trigger(rst, timerPort);
        trigger(rRequest, networkPort);
    }

    /**
     * Answer a {@link GradientShuffleMessage.Request} with the nodes from the view preferred by
     * the inquirer.
     */
    final Handler<GradientShuffleMessage.Request> handleShuffleRequest = new Handler<GradientShuffleMessage.Request>() {
        @Override
        public void handle(GradientShuffleMessage.Request event) {
            Set<VodDescriptor> vodDescriptors = event.getVodDescriptors();

            VodDescriptor exchangePartnerDescriptor = null;
            for (VodDescriptor vodDescriptor : vodDescriptors) {
                if (vodDescriptor.getVodAddress().equals(event.getVodSource())) {
                    exchangePartnerDescriptor = vodDescriptor;
                    break;
                }
            }

            // Requester didn't follow the protocol
            if (exchangePartnerDescriptor == null) {
                return;
            }

            Set<VodDescriptor> exchangeNodes = gradientView.getExchangeDescriptors(exchangePartnerDescriptor, config.getShuffleLength());
            GradientShuffleMessage.Response rResponse = new GradientShuffleMessage.Response(self.getAddress(), event.getVodSource(), event.getTimeoutId(), exchangeNodes);
            trigger(rResponse, networkPort);

            gradientView.merge(vodDescriptors);
            sendGradientViewChange();
        }
    };

    /**
     * Merge the entries from the response to the view.
     */
    final Handler<GradientShuffleMessage.Response> handleShuffleResponse = new Handler<GradientShuffleMessage.Response>() {
        @Override
        public void handle(GradientShuffleMessage.Response event) {
            UUID shuffleId = (UUID) event.getTimeoutId();
            if (outstandingShuffles.containsKey(shuffleId)) {
                outstandingShuffles.remove(shuffleId);
                CancelTimeout ct = new CancelTimeout(shuffleId);
                trigger(ct, timerPort);
            }

            gradientView.merge(event.getVodDescriptors());
            sendGradientViewChange();
        }
    };

    /**
     * Broadcast the current view to the listening components.
     */
    void sendGradientViewChange() {
        if (gradientView.isChanged()) {
            // Create a copy so components don't affect each other
            SortedSet<VodDescriptor> view = new TreeSet<VodDescriptor>(gradientView.getAll());
            trigger(new GradientViewChangePort.GradientViewChanged(gradientView.isConverged(), view), gradientViewChangePort);
        }
    }

    /**
     * Remove a node from the view if it didn't respond to a request.
     */
    final Handler<ShuffleRequestTimeout> handleShuffleRequestTimeout = new Handler<ShuffleRequestTimeout>() {
        @Override
        public void handle(ShuffleRequestTimeout event) {
            UUID rTimeoutId = (UUID) event.getTimeoutId();
            VodAddress deadNode = outstandingShuffles.remove(rTimeoutId);

            if (deadNode != null) {
                gradientView.remove(deadNode);
            }
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

            incrementRoutingTableAge();
            addRoutingTableEntries(sample);

            // Remove all samples from other partitions
            Iterator<VodDescriptor> iterator = sample.iterator();
            while (iterator.hasNext()) {
                // TODO do not access constants
                if(iterator.next().getVodAddress().getId() % MsConfig.SEARCH_NUM_PARTITIONS != self.getAddress().getPartitionId())  {
                    iterator.remove();
                }
            }

            // Shuffle with one sample from our partition
            if (sample.size() > 0) {
                int n = random.nextInt(sample.size());
                initiateShuffle(sample.get(n));
            }
        }
    };

    private void incrementRoutingTableAge() {
        for (TreeSet<VodDescriptor> bucket : routingTable.values()) {
            for (VodDescriptor descriptor : bucket) {
                descriptor.incrementAndGetAge();
            }
        }
    }

    private void addRoutingTableEntries(Collection<VodDescriptor> nodes) {
        for (VodDescriptor p : nodes) {
            // TODO do not access constants
            int samplePartition = p.getVodAddress().getId() % MsConfig.SEARCH_NUM_PARTITIONS;
            TreeSet<VodDescriptor> bucket = routingTable.get(samplePartition);
            if (bucket == null) {
                bucket = new TreeSet<VodDescriptor>(peerAgeComparator);
                routingTable.put(samplePartition, bucket);
            }

            bucket.add(p);
            // keep the freshest descriptors in this partition
            while (bucket.size() > config.getMaxNumRoutingEntries()) {
                bucket.pollLast();
            }
        }
    }
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

    private IndexEntry entryToAdd;
    private boolean leaderFound;
    final Handler<GradientRoutingPort.AddIndexEntryRequest> handleAddIndexEntryRequest = new Handler<GradientRoutingPort.AddIndexEntryRequest>() {
        @Override
        public void handle(GradientRoutingPort.AddIndexEntryRequest event) {
            leaderFound = false;
            entryToAdd = event.getEntry();

            // TODO should ask the best nodes
            SortedSet<VodDescriptor> higherNodes = gradientView.getHigherUtilityNodes();
            Iterator<VodDescriptor> iterator = higherNodes.iterator();
            for (int i = 0; i < higherNodes.size() && i < LeaderLookupMessage.A && iterator.hasNext(); i++) {
                trigger(new LeaderLookupMessage.Request(self.getAddress(), iterator.next().getVodAddress(), event.getTimeoutId()), networkPort);
            }
        }
    };

    final Handler<LeaderLookupMessage.Request> handleLeaderLookupRequest = new Handler<LeaderLookupMessage.Request>() {
        @Override
        public void handle(LeaderLookupMessage.Request event) {
            SortedSet<VodDescriptor> higherNodes = gradientView.getHigherUtilityNodes();
            ArrayList<VodDescriptor> vodDescriptors = new ArrayList<VodDescriptor>();

            // TODO should return the best nodes
            Iterator<VodDescriptor> iterator = higherNodes.iterator();
            for (int i = 0; i < LeaderLookupMessage.K && iterator.hasNext(); i++) {
                vodDescriptors.add(iterator.next());
            }

            trigger(new LeaderLookupMessage.Response(self.getAddress(), event.getVodSource(), event.getTimeoutId(), leader, vodDescriptors), networkPort);
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
                List<VodDescriptor> higherNodes = event.getVodDescriptors();
                Collections.sort(higherNodes, utilityComparator);
                for (int i = 0; i < higherNodes.size() && i < LeaderLookupMessage.A; i++) {
                    trigger(new LeaderLookupMessage.Request(self.getAddress(), higherNodes.get(i).getVodAddress(), event.getTimeoutId()), networkPort);
                }
            }
        }
    };

    final Handler<GradientRoutingPort.ReplicationPrepairCommitRequest> handleReplicationPrepairCommit = new Handler<GradientRoutingPort.ReplicationPrepairCommitRequest>() {
        @Override
        public void handle(GradientRoutingPort.ReplicationPrepairCommitRequest event) {
            TreeSet<VodDescriptor> bucket = routingTable.get(self.getAddress().getPartitionId());
            // TODO do not access constants
            int i = bucket.size() > MsConfig.SEARCH_REPLICATION_MAXIMUM ? MsConfig.SEARCH_REPLICATION_MAXIMUM : bucket.size();
            for (VodDescriptor peer : bucket) {
                if (i == 0) {
                    break;
                }
                trigger(new ReplicationPrepairCommitMessage.Request(self.getAddress(), peer.getVodAddress(), event.getTimeoutId(), event.getEntry()), networkPort);
                i--;
            }
        }
    };

    final Handler<GradientRoutingPort.ReplicationCommit> handleReplicationCommit = new Handler<GradientRoutingPort.ReplicationCommit>() {
        @Override
        public void handle(GradientRoutingPort.ReplicationCommit event) {
            TreeSet<VodDescriptor> bucket = routingTable.get(self.getAddress().getPartitionId());
            for (VodDescriptor peer : bucket) {
                trigger(new ReplicationCommitMessage.Request(self.getAddress(), peer.getVodAddress(), event.getTimeoutId(), event.getIndexEntryId(), event.getSignature()), networkPort);
            }
        }
    };

    final Handler<GradientRoutingPort.IndexExchangeRequest> handleIndexExchangeRequest = new Handler<GradientRoutingPort.IndexExchangeRequest>() {
        @Override
        public void handle(GradientRoutingPort.IndexExchangeRequest event) {
            TreeSet<VodDescriptor> bucket = routingTable.get(self.getAddress().getPartitionId());
            if (bucket != null) {
                int n = random.nextInt(bucket.size());

                trigger(new IndexExchangeMessage.Request(self.getAddress(), ((VodDescriptor) bucket.toArray()[n]).getVodAddress(),
                        UUID.nextUUID(), event.getLowestMissingIndexEntry(), event.getExistingEntries(), 0, 0), networkPort);
            }
        }
    };

    final Handler<GradientRoutingPort.SearchRequest> handleSearchRequest = new Handler<GradientRoutingPort.SearchRequest>() {
        @Override
        public void handle(GradientRoutingPort.SearchRequest event) {
            int i = 0;
            for (SortedSet<VodDescriptor> bucket : routingTable.values()) {
                // Skip local partition
                if (i == self.getAddress().getPartitionId()) {
                    i++;
                    continue;
                }

                int n = random.nextInt(bucket.size());

                trigger(new SearchMessage.Request(self.getAddress(), ((VodDescriptor) bucket.toArray()[n]).getVodAddress(), event.getTimeoutId(), event.getPattern()), networkPort);
                i++;
            }
        }
    };

    /**
     * Handles broadcast public key request from Search component
     */
    final Handler<PublicKeyBroadcast> handlePublicKeyBroadcast = new Handler<PublicKeyBroadcast>() {
        @Override
        public void handle(PublicKeyBroadcast publicKeyBroadcast) {
            PublicKey key = publicKeyBroadcast.getPublicKey();

            for(VodDescriptor item : gradientView.getAll())
                trigger(new PublicKeyMessage(self.getAddress(), item.getVodAddress().getNodeAddress(), key), networkPort);
        }
    };

    /**
     * Handles PublicKey message and broadcasts it down to the gradient
     */
    final Handler<PublicKeyMessage> handlePublicKeyMessage = new Handler<PublicKeyMessage>() {
        @Override
        public void handle(PublicKeyMessage publicKeyMessage) {
            PublicKey key = publicKeyMessage.getPublicKey();

            trigger(new PublicKeyBroadcast(key), publicKeyPort);

            for(VodDescriptor item : gradientView.getLowerUtilityNodes())
                trigger(new PublicKeyMessage(publicKeyMessage.getVodSource(), item.getVodAddress().getNodeAddress(), key), networkPort);
        }
    };

    // If you call this method with a list of entries, it will
    // return a single node, weighted towards the 'best' node (as defined by
    // ComparatorById) with the temperature controlling the weighting.
    // A temperature of '1.0' will be greedy and always return the best node.
    // A temperature of '0.000001' will return a random node.
    // A temperature of '0.0' will throw a divide by zero exception :)
    // Reference:
    // http://webdocs.cs.ualberta.ca/~sutton/book/2/node4.html
    private VodDescriptor getSoftMaxAddress(List<VodDescriptor> entries) {
        Collections.sort(entries, utilityComparator);

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
}
