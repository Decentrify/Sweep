package se.sics.ms.gradient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import se.sics.ms.messages.*;
import se.sics.ms.timeout.IndividualTimeout;
import se.sics.ms.types.IndexEntry;

import java.security.PublicKey;
import java.util.*;


/**
 * Component creating a gradient network from Croupier samples according to a
 * preference function.
 */
public final class Gradient extends ComponentDefinition {

    private static final Logger logger = LoggerFactory.getLogger(Gradient.class);

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

    // This is a routing table maintaining a a list of descriptors for each category its partitions.
    private Map<IndexEntry.Category, Map<Integer, TreeSet<VodDescriptor>>> routingTable;
    Comparator<VodDescriptor> peerAgeComparator = new Comparator<VodDescriptor>() {
        @Override
        public int compare(VodDescriptor t0, VodDescriptor t1) {
            if (t0.getVodAddress().equals(t1.getVodAddress())) {
                return 0;
            } else if (!t0.isConnected() && t1.isConnected()) {
                return 1;
            } else if (t0.isConnected() && !t1.isConnected()) {
                return -1;
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
        subscribe(handleReplicationPrepareCommit, gradientRoutingPort);
        subscribe(handleSearchRequest, gradientRoutingPort);
        subscribe(handleReplicationCommit, gradientRoutingPort);
        subscribe(handleLeaderLookupRequestTimeout, timerPort);
        subscribe(handleSearchResponse, networkPort);
        subscribe(handleSearchRequestTimeout, timerPort);
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
            routingTable = new HashMap<IndexEntry.Category, Map<Integer, TreeSet<VodDescriptor>>>();
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
        rst.setTimeoutEvent(new GradientShuffleMessage.RequestTimeout(rst, self.getId()));
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
    final Handler<GradientShuffleMessage.RequestTimeout> handleShuffleRequestTimeout = new Handler<GradientShuffleMessage.RequestTimeout>() {
        @Override
        public void handle(GradientShuffleMessage.RequestTimeout event) {
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
        for (Map<Integer, TreeSet<VodDescriptor>> categoryRoutingMap : routingTable.values()) {
            for (TreeSet<VodDescriptor> bucket : categoryRoutingMap.values()) {
                for (VodDescriptor descriptor : bucket) {
                    descriptor.incrementAndGetAge();
                }
            }
        }
    }

    private void addRoutingTableEntries(Collection<VodDescriptor> nodes) {
        for (VodDescriptor vodDescriptor : nodes) {
            IndexEntry.Category category = categoryFromCategoryId(vodDescriptor.getVodAddress().getCategoryId());
            int partition = vodDescriptor.getVodAddress().getPartitionId();

            Map<Integer, TreeSet<VodDescriptor>> categoryRoutingMap = routingTable.get(category);
            if (categoryRoutingMap == null) {
                categoryRoutingMap = new HashMap<Integer, TreeSet<VodDescriptor>>();
                routingTable.put(category, categoryRoutingMap);
            }

            TreeSet<VodDescriptor> bucket = categoryRoutingMap.get(partition);
            if (bucket == null) {
                bucket = new TreeSet<VodDescriptor>(peerAgeComparator);
                categoryRoutingMap.put(partition, bucket);
            }

            bucket.add(vodDescriptor);
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

    private IndexEntry indexEntryToAdd;
    private TimeoutId addIndexEntryRequestTimeoutId;
    final private HashSet<VodDescriptor> queriedNodes = new HashSet<VodDescriptor>();
    final private HashMap<TimeoutId, VodDescriptor> openRequests = new HashMap<TimeoutId, VodDescriptor>();
    final private HashMap<VodAddress, Integer> locatedLeaders = new HashMap<VodAddress, Integer>();

    final Handler<GradientRoutingPort.AddIndexEntryRequest> handleAddIndexEntryRequest = new Handler<GradientRoutingPort.AddIndexEntryRequest>() {
        @Override
        public void handle(GradientRoutingPort.AddIndexEntryRequest event) {
            // Random addId used for finding the right partition
            int addId = random.nextInt(Integer.MAX_VALUE);
            event.getEntry().setId(addId);

            IndexEntry.Category selfCategory = categoryFromCategoryId(self.getAddress().getCategoryId());
            IndexEntry.Category addCategory = event.getEntry().getCategory();
            int selfPartition = self.getAddress().getPartitionId();
            // TODO Do not access the search constant
            int addPartition = addId % MsConfig.SEARCH_NUM_PARTITIONS;

            indexEntryToAdd = event.getEntry();
            addIndexEntryRequestTimeoutId = event.getTimeoutId();
            locatedLeaders.clear();
            queriedNodes.clear();
            openRequests.clear();

            if (addCategory == selfCategory && selfPartition == addPartition && leader) {
                trigger(new AddIndexEntryMessage.Request(self.getAddress(), self.getAddress(), event.getTimeoutId(), indexEntryToAdd), networkPort);
                return;
            }

            Iterator<VodDescriptor> iterator;
            if (addCategory == selfCategory && selfPartition == addPartition) {
                NavigableSet<VodDescriptor> startNodes = new TreeSet<VodDescriptor>(utilityComparator);
                startNodes.addAll(gradientView.getAll());
                // Higher utility nodes are further away in the sorted set
                iterator = startNodes.descendingIterator();
            } else {
                Map<Integer, TreeSet<VodDescriptor>> partitions = routingTable.get(addCategory);
                if (partitions == null) {
                    logger.info("{} handleAddIndexEntryRequest: no partition for category {} ", self.getAddress(), addCategory);
                    return;
                }

                NavigableSet<VodDescriptor> startNodes = partitions.get(addPartition);
                if (startNodes == null) {
                    logger.info("{} handleAddIndexEntryRequest: no nodes for partition {} ", self.getAddress(), addPartition);
                    return;
                }

                iterator = startNodes.iterator();
            }

            for (int i = 0; i < LeaderLookupMessage.QueryLimit && iterator.hasNext(); i++) {
                VodDescriptor node = iterator.next();
                sendLeaderLookupRequest(node);
            }
        }
    };

    final Handler<LeaderLookupMessage.RequestTimeout> handleLeaderLookupRequestTimeout = new Handler<LeaderLookupMessage.RequestTimeout>() {
        @Override
        public void handle(LeaderLookupMessage.RequestTimeout event) {
            VodDescriptor unresponsiveNode = openRequests.remove(event.getTimeoutId());

            if (unresponsiveNode == null) {
                logger.warn("{} bogus timeout with id: {}", self.getAddress(), event.getTimeoutId());
                return;
            }

            logger.info("{}: {} did not response to LeaderLookupRequest", self.getAddress(), unresponsiveNode);
            if (indexEntryToAdd.getCategory() == categoryFromCategoryId(self.getAddress().getCategoryId())
                    && indexEntryToAdd.getId() % MsConfig.SEARCH_NUM_PARTITIONS == self.getAddress().getPartitionId()) {
                gradientView.remove(unresponsiveNode.getVodAddress());
            } else {
                IndexEntry.Category category = categoryFromCategoryId(unresponsiveNode.getVodAddress().getCategoryId());
                Map<Integer, TreeSet<VodDescriptor>> partitions = routingTable.get(category);
                TreeSet<VodDescriptor> bucket = partitions.get(unresponsiveNode.getVodAddress().getPartitionId());
                bucket.remove(unresponsiveNode);
            }
        }
    };

    final Handler<LeaderLookupMessage.Request> handleLeaderLookupRequest = new Handler<LeaderLookupMessage.Request>() {
        @Override
        public void handle(LeaderLookupMessage.Request event) {
            TreeSet<VodDescriptor> higherNodes = new TreeSet<VodDescriptor>(gradientView.getHigherUtilityNodes());
            ArrayList<VodDescriptor> vodDescriptors = new ArrayList<VodDescriptor>();

            // Higher utility nodes are further away in the sorted set
            Iterator<VodDescriptor> iterator = higherNodes.descendingIterator();
            while (vodDescriptors.size() < LeaderLookupMessage.ResponseLimit && iterator.hasNext()) {
                vodDescriptors.add(iterator.next());
            }

            // Some space left, also return lower nodes
            if (vodDescriptors.size() < LeaderLookupMessage.ResponseLimit) {
                TreeSet<VodDescriptor> lowerNodes = new TreeSet<VodDescriptor>(gradientView.getHigherUtilityNodes());
                iterator = lowerNodes.descendingIterator();
                while (vodDescriptors.size() < LeaderLookupMessage.ResponseLimit && iterator.hasNext()) {
                    vodDescriptors.add(iterator.next());
                }
            }

            trigger(new LeaderLookupMessage.Response(self.getAddress(), event.getVodSource(), event.getTimeoutId(), leader, vodDescriptors), networkPort);
        }
    };

    final Handler<LeaderLookupMessage.Response> handleLeaderLookupResponse = new Handler<LeaderLookupMessage.Response>() {
        @Override
        public void handle(LeaderLookupMessage.Response event) {
            if (openRequests.containsKey(event.getTimeoutId()) == false) {
                return;
            }

            CancelTimeout cancelTimeout = new CancelTimeout(event.getTimeoutId());
            trigger(cancelTimeout, timerPort);
            openRequests.remove(event.getTimeoutId());

            if (event.isLeader()) {

                VodAddress source = event.getVodSource();
                Integer numberOfAnswers;
                if (locatedLeaders.containsKey(source)) {
                    numberOfAnswers = locatedLeaders.get(event.getVodSource()) + 1;
                } else {
                    numberOfAnswers = 1;
                }
                locatedLeaders.put(event.getVodSource(), numberOfAnswers);
            } else {
                List<VodDescriptor> higherUtilityNodes = event.getVodDescriptors();

                if (higherUtilityNodes.size() > LeaderLookupMessage.QueryLimit) {
                    Collections.sort(higherUtilityNodes, utilityComparator);
                    // Higher utility nodes are further away
                    Collections.reverse(higherUtilityNodes);
                }

                // If the lowest returned nodes is an announced leader, increment it's counter
                VodDescriptor first = higherUtilityNodes.get(0);
                if (locatedLeaders.containsKey(first.getVodAddress())) {
                    Integer numberOfAnswers = locatedLeaders.get(first.getVodAddress()) + 1;
                    locatedLeaders.put(first.getVodAddress(), numberOfAnswers);
                }

                Iterator<VodDescriptor> iterator = higherUtilityNodes.iterator();
                for (int i = 0; i < LeaderLookupMessage.QueryLimit && iterator.hasNext(); i++) {
                    VodDescriptor node = iterator.next();
                    // Don't query nodes twice
                    if (queriedNodes.contains(node)) {
                        i--;
                        continue;
                    }
                    sendLeaderLookupRequest(node);
                }
            }

            // Check it a quorum was reached
            for (VodAddress locatedLeader : locatedLeaders.keySet()) {
                if (locatedLeaders.get(locatedLeader) > LeaderLookupMessage.QueryLimit / 2) {
                    trigger(new AddIndexEntryMessage.Request(self.getAddress(), locatedLeader, addIndexEntryRequestTimeoutId, indexEntryToAdd), networkPort);
                }
            }
        }
    };

    private void sendLeaderLookupRequest(VodDescriptor node) {
        ScheduleTimeout scheduleTimeout = new ScheduleTimeout(config.getLeaderLookupTimeout());
        scheduleTimeout.setTimeoutEvent(new LeaderLookupMessage.RequestTimeout(scheduleTimeout, self.getId()));
        openRequests.put(scheduleTimeout.getTimeoutEvent().getTimeoutId(), node);
        trigger(scheduleTimeout, timerPort);

        queriedNodes.add(node);
        trigger(new LeaderLookupMessage.Request(self.getAddress(), node.getVodAddress(), scheduleTimeout.getTimeoutEvent().getTimeoutId()), networkPort);
    }

    final Handler<GradientRoutingPort.ReplicationPrepareCommitRequest> handleReplicationPrepareCommit = new Handler<GradientRoutingPort.ReplicationPrepareCommitRequest>() {
        @Override
        public void handle(GradientRoutingPort.ReplicationPrepareCommitRequest event) {
            for (VodDescriptor peer : gradientView.getLowerUtilityNodes()) {
                trigger(new ReplicationPrepareCommitMessage.Request(self.getAddress(), peer.getVodAddress(), event.getTimeoutId(), event.getEntry()), networkPort);
            }
        }
    };

    final Handler<GradientRoutingPort.ReplicationCommit> handleReplicationCommit = new Handler<GradientRoutingPort.ReplicationCommit>() {
        @Override
        public void handle(GradientRoutingPort.ReplicationCommit event) {
            for (VodDescriptor peer : gradientView.getLowerUtilityNodes()) {
                trigger(new ReplicationCommitMessage.Request(self.getAddress(), peer.getVodAddress(), event.getTimeoutId(), event.getIndexEntryId(), event.getSignature()), networkPort);
            }
        }
    };

    final Handler<GradientRoutingPort.IndexExchangeRequest> handleIndexExchangeRequest = new Handler<GradientRoutingPort.IndexExchangeRequest>() {
        @Override
        public void handle(GradientRoutingPort.IndexExchangeRequest event) {
            Map<Integer, TreeSet<VodDescriptor>> categoryRoutingMap = routingTable.get(categoryFromCategoryId(self.getAddress().getCategoryId()));
            if (categoryRoutingMap == null) {
                logger.trace("{} has no nodes to exchange indexes with", self.getAddress());
                return;
            }

            TreeSet<VodDescriptor> bucket = categoryRoutingMap.get(self.getAddress().getPartitionId());
            if (bucket == null) {
                logger.trace("{} has no nodes to exchange indexes with", self.getAddress());
                return;
            }

            int n = random.nextInt(bucket.size());
            trigger(new IndexExchangeMessage.Request(self.getAddress(), ((VodDescriptor) bucket.toArray()[n]).getVodAddress(),
                    UUID.nextUUID(), event.getLowestMissingIndexEntry(), event.getExistingEntries(), 0, 0), networkPort);
        }
    };

    final Handler<GradientRoutingPort.SearchRequest> handleSearchRequest = new Handler<GradientRoutingPort.SearchRequest>() {
        @Override
        public void handle(GradientRoutingPort.SearchRequest event) {
            IndexEntry.Category category = event.getPattern().getCategory();
            Map<Integer, TreeSet<VodDescriptor>> categoryRoutingMap = routingTable.get(category);

            if (categoryRoutingMap == null) {
                return;
            }

            for (Integer partition : categoryRoutingMap.keySet()) {
                // Skip local partition
                if (partition == self.getAddress().getPartitionId() && category == categoryFromCategoryId(self.getAddress().getCategoryId())) {
                    continue;
                }

                SortedSet<VodDescriptor> bucket = categoryRoutingMap.get(partition);
                Iterator<VodDescriptor> iterator = bucket.iterator();
                for (int i = 0; i < config.getSearchParallelism() && iterator.hasNext(); i++) {
                    VodDescriptor vodDescriptor = iterator.next();
                    ScheduleTimeout scheduleTimeout = new ScheduleTimeout(event.getQueryTimeout());
                    scheduleTimeout.setTimeoutEvent(new SearchMessage.RequestTimeout(scheduleTimeout, self.getId(), vodDescriptor));
                    trigger(scheduleTimeout, timerPort);
                    trigger(new SearchMessage.Request(self.getAddress(), vodDescriptor.getVodAddress(),
                            scheduleTimeout.getTimeoutEvent().getTimeoutId(), event.getTimeoutId(), event.getPattern()), networkPort);
                }
            }
        }
    };

    final Handler<SearchMessage.Response> handleSearchResponse = new Handler<SearchMessage.Response>() {
        @Override
        public void handle(SearchMessage.Response event) {
            CancelTimeout cancelTimeout = new CancelTimeout(event.getTimeoutId());
            trigger(cancelTimeout, timerPort);
        }
    };

    final Handler<SearchMessage.RequestTimeout> handleSearchRequestTimeout = new Handler<SearchMessage.RequestTimeout>() {
        @Override
        public void handle(SearchMessage.RequestTimeout event) {
            IndexEntry.Category category = categoryFromCategoryId(event.getVodDescriptor().getVodAddress().getCategoryId());
            Map<Integer, TreeSet<VodDescriptor>> categoryRoutingMap = routingTable.get(category);
            SortedSet<VodDescriptor> bucket = categoryRoutingMap.get(event.getVodDescriptor().getVodAddress().getPartitionId());
            bucket.remove(event.getVodDescriptor());
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

    private IndexEntry.Category categoryFromCategoryId(int categoryId) {
        return IndexEntry.Category.values()[categoryId];
    }

    // If you call this method with a list of entries, it will
    // return a single node, weighted towards the 'best' node (as defined by
    // ComparatorById) with the temperature controlling the weighting.
    // QueryLimit temperature of '1.0' will be greedy and always return the best node.
    // QueryLimit temperature of '0.000001' will return a random node.
    // QueryLimit temperature of '0.0' will throw a divide by zero exception :)
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
