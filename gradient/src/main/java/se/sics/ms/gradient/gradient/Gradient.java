package se.sics.ms.gradient.gradient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.co.FailureDetectorPort;
import se.sics.gvod.common.RTTStore;
import se.sics.gvod.common.Self;
import se.sics.gvod.common.SelfImpl;
import se.sics.gvod.common.net.RttStats;
import se.sics.gvod.config.GradientConfiguration;
import se.sics.gvod.croupier.PeerSamplePort;
import se.sics.gvod.croupier.events.CroupierSample;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.VodNetwork;
import se.sics.gvod.timer.*;
import se.sics.gvod.timer.Timer;
import se.sics.gvod.timer.UUID;
import se.sics.kompics.*;
import se.sics.ms.common.MsSelfImpl;
import se.sics.ms.common.TransportHelper;
import se.sics.ms.configuration.MsConfig;
import se.sics.ms.gradient.control.CheckLeaderInfoUpdate;
import se.sics.ms.gradient.control.CheckPartitionInfoHashUpdate;
import se.sics.ms.gradient.control.ControlMessageEnum;
import se.sics.ms.gradient.control.ControlMessageInternal;
import se.sics.ms.gradient.events.*;
import se.sics.ms.gradient.misc.UtilityComparator;
import se.sics.ms.gradient.ports.GradientRoutingPort;
import se.sics.ms.gradient.ports.GradientViewChangePort;
import se.sics.ms.gradient.ports.LeaderStatusPort;
import se.sics.ms.gradient.ports.LeaderStatusPort.LeaderStatus;
import se.sics.ms.gradient.ports.LeaderStatusPort.NodeCrashEvent;
import se.sics.ms.gradient.ports.PublicKeyPort;
import se.sics.ms.messages.*;
import se.sics.ms.snapshot.Snapshot;
import se.sics.ms.timeout.IndividualTimeout;
import se.sics.ms.types.*;
import se.sics.ms.types.OverlayId;
import se.sics.ms.util.Pair;
import se.sics.ms.util.PartitionHelper;

import java.security.PublicKey;
import java.util.*;

import static se.sics.ms.util.PartitionHelper.adjustDescriptorsToNewPartitionId;
import static se.sics.ms.util.PartitionHelper.updateBucketsInRoutingTable;

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
    Positive<FailureDetectorPort> fdPort = requires(FailureDetectorPort.class);
    Negative<LeaderStatusPort> leaderStatusPort = negative(LeaderStatusPort.class);
    Positive<PublicKeyPort> publicKeyPort = positive(PublicKeyPort.class);
    Negative<GradientRoutingPort> gradientRoutingPort = negative(GradientRoutingPort.class);
    private MsSelfImpl self;
    private GradientConfiguration config;
    private Random random;
    private GradientView gradientView;
    private UtilityComparator utilityComparator = new UtilityComparator();
    private Map<UUID, VodAddress> outstandingShuffles;
    private boolean leader;
    private VodAddress leaderAddress;
    private PublicKey leaderPublicKey;
    private Map<Integer, Long> shuffleTimes = new HashMap<Integer, Long>();
    int latestRttRingBufferPointer = 0;
    private long[] latestRtts;
    // This is a routing table maintaining a a list of descriptors for each category and its partitions.
    private Map<MsConfig.Categories, Map<Integer, HashSet<SearchDescriptor>>> routingTable;

    private LinkedList<PartitionHelper.PartitionInfo> partitionHistory;
    private static final int HISTORY_LENGTH = 5;

    Comparator<SearchDescriptor> peerConnectivityComparator = new Comparator<SearchDescriptor>() {
        @Override
        public int compare(SearchDescriptor t0, SearchDescriptor t1) {
            if (t0.getVodAddress().equals(t1.getVodAddress())) {
                return 0;
            } else if (t0.isConnected() && t1.isConnected()) {
                return compareAvgRtt(t0, t1);
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

        private int compareAvgRtt(SearchDescriptor t0, SearchDescriptor t1) {
            RTTStore.RTT rtt0 = RTTStore.getRtt(t0.getId(), t0.getVodAddress());
            RTTStore.RTT rtt1 = RTTStore.getRtt(t1.getId(), t1.getVodAddress());

            if (rtt0 == null || rtt1 == null) {
                return 0;
            }

            RttStats rttStats0 = rtt0.getRttStats();
            RttStats rttStats1 = rtt1.getRttStats();
            if (rttStats0.getAvgRTT() == rttStats1.getAvgRTT()) {
                return 0;
            } else if (rttStats0.getAvgRTT() > rttStats1.getAvgRTT()) {
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

    public Gradient(GradientInit init) {
        doInit(init);
        subscribe(handleStart, control);
        subscribe(handleRound, timerPort);
        subscribe(handleShuffleRequestTimeout, timerPort);
        subscribe(handleCroupierSample, croupierSamplePort);
        subscribe(handleShuffleResponse, networkPort);
        subscribe(handleShuffleRequest, networkPort);
        subscribe(handleLeaderLookupRequest, networkPort);
        subscribe(handleLeaderLookupResponse, networkPort);
        subscribe(handleLeaderStatus, leaderStatusPort);
        subscribe(handleNodeCrash, leaderStatusPort);
        subscribe(handleLeaderUpdate, leaderStatusPort);
        subscribe(handlePublicKeyBroadcast, publicKeyPort);
        subscribe(handleAddIndexEntryRequest, gradientRoutingPort);
        subscribe(handleIndexHashExchangeRequest, gradientRoutingPort);
        subscribe(handleReplicationPrepareCommit, gradientRoutingPort);
        subscribe(handleSearchRequest, gradientRoutingPort);
        subscribe(handleReplicationCommit, gradientRoutingPort);
        subscribe(handleLeaderLookupRequestTimeout, timerPort);
        subscribe(handleSearchResponse, networkPort);
        subscribe(handleSearchRequestTimeout, timerPort);
        subscribe(handleViewSizeRequest, gradientRoutingPort);
        subscribe(handlePartitioningUpdate, gradientRoutingPort);

        subscribe(handleLeaderGroupInformationRequest, gradientRoutingPort);
        subscribe(handleFailureDetector, fdPort);
		subscribe(handlerControlMessageExchangeInitiation, gradientRoutingPort);
        subscribe(handlerControlMessageInternalRequest, gradientRoutingPort);
        subscribe(handlerCheckPartitioningInfoRequest, gradientRoutingPort);

    }
    /**
     * Initialize the state of the component.
     */
    private void doInit(GradientInit init) {

        self = (MsSelfImpl)init.getSelf();
        config = init.getConfiguration();
        outstandingShuffles = Collections.synchronizedMap(new HashMap<UUID, VodAddress>());
        random = new Random(init.getConfiguration().getSeed());
        gradientView = new GradientView(self, config.getViewSize(), config.getConvergenceTest(), config.getConvergenceTestRounds());
        routingTable = new HashMap<MsConfig.Categories, Map<Integer, HashSet<SearchDescriptor>>>();
        leader = false;
        leaderAddress = null;
        latestRtts = new long[config.getLatestRttStoreLimit()];
        partitionHistory = new LinkedList<PartitionHelper.PartitionInfo>();      // Store the history of partitions but upto a specified level.
    }

    public Handler<Start> handleStart = new Handler<Start>() {

        @Override
        public void handle(Start e) {
            SchedulePeriodicTimeout rst = new SchedulePeriodicTimeout(config.getShufflePeriod(), config.getShufflePeriod());

            rst.setTimeoutEvent(new GradientRound(rst, self.getId()));
            trigger(rst, timerPort);
        }
    };

    private void removeNodeFromRoutingTable(OverlayAddress nodeToRemove)
    {
        MsConfig.Categories category = categoryFromCategoryId(nodeToRemove.getCategoryId());
        Map<Integer, HashSet<SearchDescriptor>> categoryRoutingMap = routingTable.get(category);
        if(categoryRoutingMap != null) {
            Set<SearchDescriptor> bucket = categoryRoutingMap.get(nodeToRemove.getPartitionId());

            if (bucket != null) {
                Iterator<SearchDescriptor> i = bucket.iterator();
                while (i.hasNext()) {
                    SearchDescriptor descriptor = i.next();

                    if (descriptor.getVodAddress().equals(nodeToRemove)) {
                        i.remove();
                        break;
                    }
                }
            }
        }
    }

    private void removeNodesFromLocalState(HashSet<VodAddress> nodesToRemove) {

        for(VodAddress suspectedNode: nodesToRemove) {

            removeNodeFromLocalState(new OverlayAddress(suspectedNode));
        }
    }
    private void removeNodeFromLocalState(OverlayAddress overlayAddress)
    {
        //remove suspected node from gradient view
        gradientView.remove(overlayAddress.getAddress());

        //remove suspected node from routing table
        removeNodeFromRoutingTable(overlayAddress);

        //remove suspected nodes from rtt store
        RTTStore.removeSamples(overlayAddress.getId(), overlayAddress.getAddress());
    }

    private void publishUnresponsiveNode(VodAddress nodeAddress)
    {
        trigger(new FailureDetectorPort.FailureDetectorEvent(nodeAddress), fdPort);
    }

    final Handler<FailureDetectorPort.FailureDetectorEvent> handleFailureDetector = new Handler<FailureDetectorPort.FailureDetectorEvent>() {

        @Override
        public void handle(FailureDetectorPort.FailureDetectorEvent event) {
            removeNodesFromLocalState(event.getSuspectedNodes());
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
    private void initiateShuffle(SearchDescriptor exchangePartner) {
        Set<SearchDescriptor> exchangeNodes = gradientView.getExchangeDescriptors(exchangePartner, config.getShuffleLength());

        ScheduleTimeout rst = new ScheduleTimeout(config.getShufflePeriod());
        rst.setTimeoutEvent(new GradientShuffleMessage.RequestTimeout(rst, self.getId()));
        UUID rTimeoutId = (UUID) rst.getTimeoutEvent().getTimeoutId();
        outstandingShuffles.put(rTimeoutId, exchangePartner.getVodAddress());

//        if(self.getId() == 726089965 && self.getPartitioningType() == VodAddress.PartitioningType.ONCE_BEFORE){
//            logger.warn(" ========== Pushing Number of Index Entries : " + self.getNumberOfIndexEntries());
//        }


        GradientShuffleMessage.Request rRequest = new GradientShuffleMessage.Request(self.getAddress(), exchangePartner.getVodAddress(), rTimeoutId, exchangeNodes);
        exchangePartner.setConnected(true);

        trigger(rst, timerPort);
        trigger(rRequest, networkPort);

        shuffleTimes.put(rTimeoutId.getId(), System.currentTimeMillis());
    }
    /**
     * Answer a {@link GradientShuffleMessage.Request} with the nodes from the
     * view preferred by the inquirer.
     */
    final Handler<GradientShuffleMessage.Request> handleShuffleRequest = new Handler<GradientShuffleMessage.Request>() {
        @Override
        public void handle(GradientShuffleMessage.Request event) {
            Set<SearchDescriptor> searchDescriptors = event.getSearchDescriptors();

            SearchDescriptor exchangePartnerDescriptor = null;
            for (SearchDescriptor searchDescriptor : searchDescriptors) {
                if (searchDescriptor.getVodAddress().equals(event.getVodSource())) {
                    exchangePartnerDescriptor = searchDescriptor;
                    break;
                }
            }

            // Requester didn't follow the protocol
            if (exchangePartnerDescriptor == null) {
                return;
            }

            Set<SearchDescriptor> exchangeNodes = gradientView.getExchangeDescriptors(exchangePartnerDescriptor, config.getShuffleLength());
            GradientShuffleMessage.Response rResponse = new GradientShuffleMessage.Response(self.getAddress(), event.getVodSource(), event.getTimeoutId(), exchangeNodes);
            trigger(rResponse, networkPort);

//            if(self.getId() == 520972445 && self.getPartitioningType() == VodAddress.PartitioningType.ONCE_BEFORE){
//
//                logger.warn("========== RECEIVED DESCRIPTORS FOR MERGING FROM:  =============== " + event.getVodSource().getId());
//                for(SearchDescriptor desc : searchDescriptors)
//                     logger.warn(" DescriptorID : " + desc.getId() + " Descriptor Overlay : " + desc.getOverlayId() + "Number of Index Entries: " + desc.getNumberOfIndexEntries());
//                logger.warn("=========== END ==========================");
//                logger.warn("");
//            }

            gradientView.merge(searchDescriptors);

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

            Collection<SearchDescriptor> sample = event.getSearchDescriptors();

            boolean isNeverBefore = self.getPartitioningType() == VodAddress.PartitioningType.NEVER_BEFORE;

            Set<SearchDescriptor> updatedSample = new HashSet<SearchDescriptor>();
            if (!isNeverBefore) {

                int bitsToCheck = self.getPartitionIdDepth();
                boolean isOnceBefore = self.getPartitioningType() == VodAddress.PartitioningType.ONCE_BEFORE;
                for (SearchDescriptor d : sample) {
                    PartitionId partitionId = PartitionHelper.determineSearchDescriptorPartition(d,
                            isOnceBefore, bitsToCheck);
                    VodAddress a = PartitionHelper.updatePartitionId(d, partitionId);
                    updatedSample.add(new SearchDescriptor(a, d));
                }
            } else {
                for (SearchDescriptor d : sample) {
                    VodAddress a = PartitionHelper.updatePartitionId(d,
                            new PartitionId(VodAddress.PartitioningType.NEVER_BEFORE, 1, 0));
                    updatedSample.add(new SearchDescriptor(a, d));
                }
            }

            // Remove all samples from other partitions
//            Iterator<SearchDescriptor> iterator = descriptors.iterator();
            Iterator<SearchDescriptor> iterator = updatedSample.iterator();
            Set<SearchDescriptor> toRemove = new HashSet<SearchDescriptor>();
            while (iterator.hasNext()) {
                SearchDescriptor d = iterator.next();
                OverlayAddress next = d.getOverlayAddress();
                if (next.getPartitionId() != self.getPartitionId()
                        || next.getPartitionIdDepth() != self.getPartitionIdDepth()
                        || next.getPartitioningType() != self.getPartitioningType()) {
//                    iterator.remove();
                    toRemove.add(d);
                }
            }
            updatedSample.removeAll(toRemove);

//            gradientView.merge(sample);
            gradientView.merge(updatedSample);
            sendGradientViewChange();

            long timeStarted = shuffleTimes.remove(event.getTimeoutId().getId());
            long rtt = System.currentTimeMillis() - timeStarted;
            RTTStore.addSample(self.getId(), event.getVodSource(), rtt);
            updateLatestRtts(rtt);
        }
    };

    /**
     * Broadcast the current view to the listening components.
     */
    void sendGradientViewChange() {
        if (gradientView.isChanged()) {
            // Create a copy so components don't affect each other
            SortedSet<SearchDescriptor> view = new TreeSet<SearchDescriptor>(gradientView.getAll());

            Iterator<SearchDescriptor> iterator = view.iterator();
            while (iterator.hasNext()) {
                OverlayAddress next = iterator.next().getOverlayAddress();
                if (next.getPartitionId() != self.getPartitionId()
                        || next.getPartitionIdDepth() != self.getPartitionIdDepth()
                        || next.getPartitioningType() != self.getPartitioningType()) {
                    iterator.remove();
                }
            }

//            if(self.getId() == 319791623)
//                logger.warn("_ISSUE: Am I Converged  ...." + gradientView.isConverged());

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

            if (deadNode == null) {
                logger.warn("{} bogus timeout with id: {}", self.getAddress(), event.getTimeoutId());
                return;
            }

            publishUnresponsiveNode(deadNode);
            shuffleTimes.remove(event.getTimeoutId().getId());
            RTTStore.removeSamples(deadNode.getId(), deadNode);
        }
    };
    /**
     * Initiate a exchange with a random node of each Croupier sample to speed
     * up convergence and prevent partitioning.
     */
    final Handler<CroupierSample> handleCroupierSample = new Handler<CroupierSample>() {
        @Override
        public void handle(CroupierSample event) {
            List<SearchDescriptor> sample = SearchDescriptor.toSearchDescriptorList(event.getNodes());
            List<SearchDescriptor> updatedSample = new ArrayList<SearchDescriptor>();

            if ((self.getPartitioningType() != VodAddress.PartitioningType.NEVER_BEFORE)) {
                boolean isOnePartition = self.getPartitioningType() == VodAddress.PartitioningType.ONCE_BEFORE;
                if (!isOnePartition) {
                    int bitsToCheck = self.getPartitionIdDepth();

                    for (SearchDescriptor d : sample) {
                        PartitionId partitionId = PartitionHelper.determineSearchDescriptorPartition(d,
                                isOnePartition, bitsToCheck);

                        VodAddress a = PartitionHelper.updatePartitionId(d, partitionId);
                        updatedSample.add(new SearchDescriptor(a, d));

                    }
                } else {
                    for (SearchDescriptor d : sample) {
                        PartitionId partitionId = PartitionHelper.determineSearchDescriptorPartition(d,
                                isOnePartition, 1);

                        VodAddress a = PartitionHelper.updatePartitionId(d, partitionId);
                        updatedSample.add(new SearchDescriptor(a, d));

                    }
                }
            }

            incrementRoutingTableAge();
//            addRoutingTableEntries(sample);
            if(self.getPartitioningType() != VodAddress.PartitioningType.NEVER_BEFORE)
                addRoutingTableEntries(updatedSample);
            else {
                updatedSample = sample;
                addRoutingTableEntries(updatedSample);
            }

            // Remove all samples from other partitions
//            Iterator<SearchDescriptor> iterator = sample.iterator();
            Iterator<SearchDescriptor> iterator = updatedSample.iterator();
            while (iterator.hasNext()) {
                OverlayAddress next = iterator.next().getOverlayAddress();
                if (next.getCategoryId() != self.getCategoryId()
                        || next.getPartitionId() != self.getPartitionId()
                        || next.getPartitionIdDepth() != self.getPartitionIdDepth()
                        || next.getPartitioningType() != self.getPartitioningType()) {
                    iterator.remove();
                }
            }

            //Merge croupier sample to have quicker convergence of gradient
            gradientView.merge(updatedSample);

            // Shuffle with one sample from our partition
            if (updatedSample.size() > 0) {
                int n = random.nextInt(updatedSample.size());
                initiateShuffle(updatedSample.get(n));
            }
        }
    };

    private void incrementRoutingTableAge() {
        for (Map<Integer, HashSet<SearchDescriptor>> categoryRoutingMap : routingTable.values()) {
            for (HashSet<SearchDescriptor> bucket : categoryRoutingMap.values()) {
                for (SearchDescriptor descriptor : bucket) {
                    descriptor.incrementAndGetAge();
                }
            }
        }
    }

    private void addRoutingTableEntries(List<SearchDescriptor> nodes) {
        for (SearchDescriptor searchDescriptor : nodes) {
            MsConfig.Categories category = categoryFromCategoryId(searchDescriptor.getOverlayId().getCategoryId());
            int partition = searchDescriptor.getOverlayAddress().getPartitionId();

            Map<Integer, HashSet<SearchDescriptor>> categoryRoutingMap = routingTable.get(category);
            if (categoryRoutingMap == null) {
                categoryRoutingMap = new HashMap<Integer, HashSet<SearchDescriptor>>();
                routingTable.put(category, categoryRoutingMap);
            }

            HashSet<SearchDescriptor> bucket = categoryRoutingMap.get(partition);
            if (bucket == null) {
                bucket = new HashSet<SearchDescriptor>();
                categoryRoutingMap.put(partition, bucket);

                //update old routing tables if see an entry from a new partition
                PartitionId newPartitionId = new PartitionId(searchDescriptor.getOverlayAddress().getPartitioningType(),
                        searchDescriptor.getOverlayAddress().getPartitionIdDepth(), searchDescriptor.getOverlayAddress().getPartitionId());
                updateBucketsInRoutingTable(newPartitionId, categoryRoutingMap, bucket);
            }

            bucket.add(searchDescriptor);
            // keep the best descriptors in this partition
            TreeSet<SearchDescriptor> sortedBucket = sortByConnectivity(bucket);
            while (bucket.size() > config.getMaxNumRoutingEntries()) {
                bucket.remove(sortedBucket.pollLast());
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
     * Updates gradient's view by removing crashed nodes from it, eg. old
     * leaders
     */
    final Handler<NodeCrashEvent> handleNodeCrash = new Handler<NodeCrashEvent>() {
        @Override
        public void handle(NodeCrashEvent event) {
            VodAddress deadNode = event.getDeadNode();

            publishUnresponsiveNode(deadNode);
        }
    };
    private IndexEntry indexEntryToAdd;
    private TimeoutId addIndexEntryRequestTimeoutId;
    final private HashSet<SearchDescriptor> queriedNodes = new HashSet<SearchDescriptor>();
    final private HashMap<TimeoutId, SearchDescriptor> openRequests = new HashMap<TimeoutId, SearchDescriptor>();
    final private HashMap<VodAddress, Integer> locatedLeaders = new HashMap<VodAddress, Integer>();
    private List<VodAddress> leadersAlreadyComunicated = new ArrayList<VodAddress>();

    final Handler<GradientRoutingPort.AddIndexEntryRequest> handleAddIndexEntryRequest = new Handler<GradientRoutingPort.AddIndexEntryRequest>() {
        @Override
        public void handle(GradientRoutingPort.AddIndexEntryRequest event) {
            MsConfig.Categories selfCategory = categoryFromCategoryId(self.getCategoryId());
            MsConfig.Categories addCategory = event.getEntry().getCategory();

            indexEntryToAdd = event.getEntry();
            addIndexEntryRequestTimeoutId = event.getTimeoutId();
            locatedLeaders.clear();
            queriedNodes.clear();
            openRequests.clear();
            leadersAlreadyComunicated.clear();

            //Entry and my overlay in the same category, add to my overlay
            if (addCategory == selfCategory) {
                if (leader) {
                    trigger(new AddIndexEntryMessage.Request(self.getAddress(), self.getAddress(), event.getTimeoutId(), event.getEntry()), networkPort);
                }

                //if we have direct pointer to the leader
                else if(leaderAddress != null) {
                    //sendLeaderLookupRequest(new SearchDescriptor(leaderAddress));
                    trigger(new AddIndexEntryMessage.Request(self.getAddress(), leaderAddress, event.getTimeoutId(), event.getEntry()), networkPort);
                }

                else
                {
                    NavigableSet<SearchDescriptor> startNodes = new TreeSet<SearchDescriptor>(utilityComparator);
                    startNodes.addAll(gradientView.getAll());

                    //Also add nodes from croupier sample to have more chances of getting higher utility nodes, this works
                    //as a finger table to random nodes
                    Map<Integer, HashSet<SearchDescriptor>> croupierPartitions = routingTable.get(selfCategory);
                    if (croupierPartitions != null && !croupierPartitions.isEmpty()) {
                        HashSet<SearchDescriptor> croupierNodes =  croupierPartitions.get(self.getPartitionId());
                        if(croupierNodes != null && !croupierNodes.isEmpty()) {
                            startNodes.addAll(croupierNodes);
                        }
                    }

                    // Higher utility nodes are further away in the sorted set
                    Iterator<SearchDescriptor> iterator = startNodes.descendingIterator();

                    for (int i = 0; i < LeaderLookupMessage.QueryLimit && iterator.hasNext(); i++) {
                        SearchDescriptor node = iterator.next();
                        sendLeaderLookupRequest(node);
                    }
                }
            }
            else {
                Map<Integer, HashSet<SearchDescriptor>> partitions = routingTable.get(addCategory);
                if (partitions == null || partitions.isEmpty()) {
                    logger.info("{} handleAddIndexEntryRequest: no partition for category {} ", self.getAddress(), addCategory);
                    return;
                }

                ArrayList<Integer> categoryPartitionsIds = new ArrayList<Integer>(partitions.keySet());
                int categoryPartitionId = (int) (Math.random() * categoryPartitionsIds.size());

                HashSet<SearchDescriptor> startNodes = partitions.get(categoryPartitionsIds.get(categoryPartitionId));
                if (startNodes == null) {
                    logger.info("{} handleAddIndexEntryRequest: no nodes for partition {} ", self.getAddress(),
                            categoryPartitionsIds.get(categoryPartitionId));
                    return;
                }

                // Need to sort it every time because values like RTT might have been changed
                SortedSet<SearchDescriptor> sortedStartNodes = sortByConnectivity(startNodes);
                Iterator iterator = sortedStartNodes.iterator();

                for (int i = 0; i < LeaderLookupMessage.QueryLimit && iterator.hasNext(); i++) {
                    SearchDescriptor node = (SearchDescriptor) iterator.next();
                    sendLeaderLookupRequest(node);
                }
            }
        }
    };
    final Handler<LeaderLookupMessage.RequestTimeout> handleLeaderLookupRequestTimeout = new Handler<LeaderLookupMessage.RequestTimeout>() {
        @Override
        public void handle(LeaderLookupMessage.RequestTimeout event) {
            SearchDescriptor unresponsiveNode = openRequests.remove(event.getTimeoutId());
            shuffleTimes.remove(event.getTimeoutId().getId());

            if (unresponsiveNode == null) {
                logger.warn("{} bogus timeout with id: {}", self.getAddress(), event.getTimeoutId());
                return;
            }

            publishUnresponsiveNode(unresponsiveNode.getVodAddress());
            logger.info("{}: {} did not response to LeaderLookupRequest", self.getAddress(), unresponsiveNode);
        }
    };
    final Handler<LeaderLookupMessage.Request> handleLeaderLookupRequest = new Handler<LeaderLookupMessage.Request>() {
        @Override
        public void handle(LeaderLookupMessage.Request event) {
            TreeSet<SearchDescriptor> higherNodes = new TreeSet<SearchDescriptor>(gradientView.getHigherUtilityNodes());
            ArrayList<SearchDescriptor> searchDescriptors = new ArrayList<SearchDescriptor>();

            // Higher utility nodes are further away in the sorted set
            Iterator<SearchDescriptor> iterator = higherNodes.descendingIterator();
            while (searchDescriptors.size() < LeaderLookupMessage.ResponseLimit && iterator.hasNext()) {
                searchDescriptors.add(iterator.next());
            }

            // Some space left, also return lower nodes
            if (searchDescriptors.size() < LeaderLookupMessage.ResponseLimit) {
                TreeSet<SearchDescriptor> lowerNodes = new TreeSet<SearchDescriptor>(gradientView.getLowerUtilityNodes());
                iterator = lowerNodes.iterator();
                while (searchDescriptors.size() < LeaderLookupMessage.ResponseLimit && iterator.hasNext()) {
                    searchDescriptors.add(iterator.next());
                }
            }

            trigger(new LeaderLookupMessage.Response(self.getAddress(), event.getVodSource(), event.getTimeoutId(), leader, searchDescriptors), networkPort);
        }
    };
    final Handler<LeaderLookupMessage.Response> handleLeaderLookupResponse = new Handler<LeaderLookupMessage.Response>() {
        @Override
        public void handle(LeaderLookupMessage.Response event) {
            if (!openRequests.containsKey(event.getTimeoutId())) {
                return;
            }

            long timeStarted = shuffleTimes.remove(event.getTimeoutId().getId());
            long rtt = System.currentTimeMillis() - timeStarted;
            RTTStore.addSample(self.getId(), event.getVodSource(), rtt);
            updateLatestRtts(rtt);

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
                List<SearchDescriptor> higherUtilityNodes = event.getSearchDescriptors();

                if (higherUtilityNodes.size() > LeaderLookupMessage.QueryLimit) {
                    Collections.sort(higherUtilityNodes, utilityComparator);
                    // Higher utility nodes are further away
                    Collections.reverse(higherUtilityNodes);
                }

                // If the lowest returned nodes is an announced leader, increment it's counter
                if (higherUtilityNodes.size() > 0) {
                    SearchDescriptor first = higherUtilityNodes.get(0);
                    if (locatedLeaders.containsKey(first.getVodAddress())) {
                        Integer numberOfAnswers = locatedLeaders.get(first.getVodAddress()) + 1;
                        locatedLeaders.put(first.getVodAddress(), numberOfAnswers);
                    }
                }

                Iterator<SearchDescriptor> iterator = higherUtilityNodes.iterator();
                for (int i = 0; i < LeaderLookupMessage.QueryLimit && iterator.hasNext(); i++) {
                    SearchDescriptor node = iterator.next();
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

                    if(!leadersAlreadyComunicated.contains(locatedLeader)){
                        trigger(new AddIndexEntryMessage.Request(self.getAddress(), locatedLeader, addIndexEntryRequestTimeoutId, indexEntryToAdd), networkPort);
                        leadersAlreadyComunicated.add(locatedLeader);
                    }
                }
            }
        }
    };

    private void sendLeaderLookupRequest(SearchDescriptor node) {
        ScheduleTimeout scheduleTimeout = new ScheduleTimeout(config.getLeaderLookupTimeout());
        scheduleTimeout.setTimeoutEvent(new LeaderLookupMessage.RequestTimeout(scheduleTimeout, self.getId()));
        scheduleTimeout.getTimeoutEvent().setTimeoutId(UUID.nextUUID());
        openRequests.put(scheduleTimeout.getTimeoutEvent().getTimeoutId(), node);
        trigger(scheduleTimeout, timerPort);

        queriedNodes.add(node);
        trigger(new LeaderLookupMessage.Request(self.getAddress(), node.getVodAddress(), scheduleTimeout.getTimeoutEvent().getTimeoutId()), networkPort);

        node.setConnected(true);
        shuffleTimes.put(scheduleTimeout.getTimeoutEvent().getTimeoutId().getId(), System.currentTimeMillis());
    }
    final Handler<GradientRoutingPort.ReplicationPrepareCommitRequest> handleReplicationPrepareCommit = new Handler<GradientRoutingPort.ReplicationPrepareCommitRequest>() {
        @Override
        public void handle(GradientRoutingPort.ReplicationPrepareCommitRequest event) {
            for (SearchDescriptor peer : gradientView.getLowerUtilityNodes()) {
                trigger(new ReplicationPrepareCommitMessage.Request(self.getAddress(), peer.getVodAddress(), event.getTimeoutId(), event.getEntry()), networkPort);
            }
        }
    };
    final Handler<GradientRoutingPort.ReplicationCommit> handleReplicationCommit = new Handler<GradientRoutingPort.ReplicationCommit>() {
        @Override
        public void handle(GradientRoutingPort.ReplicationCommit event) {
            for (SearchDescriptor peer : gradientView.getLowerUtilityNodes()) {
                trigger(new ReplicationCommitMessage.Request(self.getAddress(), peer.getVodAddress(), event.getTimeoutId(), event.getIndexEntryId(), event.getSignature()), networkPort);
            }
        }
    };
    final Handler<GradientRoutingPort.IndexHashExchangeRequest> handleIndexHashExchangeRequest = new Handler<GradientRoutingPort.IndexHashExchangeRequest>() {
        @Override
        public void handle(GradientRoutingPort.IndexHashExchangeRequest event) {
            ArrayList<SearchDescriptor> nodes = new ArrayList<SearchDescriptor>(gradientView.getHigherUtilityNodes());
            if (nodes.isEmpty() || nodes.size() < event.getNumberOfRequests()) {
                // TODO: Revert Back debug check.
                logger.debug(" {}: Not enough nodes to perform Index Hash Exchange." + self.getAddress().getId());
                return;
            }

            HashSet<VodAddress> nodesSelectedForExchange = new HashSet<VodAddress>();

            for (int i = 0; i < event.getNumberOfRequests(); i++) {
                int n = random.nextInt(nodes.size());
                SearchDescriptor node = nodes.get(n);
                nodes.remove(node);

                nodesSelectedForExchange.add(node.getVodAddress());

                trigger(new IndexHashExchangeMessage.Request(self.getAddress(), node.getVodAddress(), event.getTimeoutId(),
                        event.getLowestMissingIndexEntry(), event.getExistingEntries()), networkPort);
            }

            trigger(new GradientRoutingPort.IndexHashExchangeResponse(nodesSelectedForExchange), gradientRoutingPort);
        }
    };
    final Handler<GradientRoutingPort.SearchRequest> handleSearchRequest = new Handler<GradientRoutingPort.SearchRequest>() {
        @Override
        public void handle(GradientRoutingPort.SearchRequest event) {
            MsConfig.Categories category = event.getPattern().getCategory();
            Map<Integer, HashSet<SearchDescriptor>> categoryRoutingMap = routingTable.get(category);

            if (categoryRoutingMap == null) {
                return;
            }
            trigger(new NumberOfPartitions(event.getTimeoutId(), categoryRoutingMap.keySet().size()), gradientRoutingPort);

            for (Integer partition : categoryRoutingMap.keySet()) {
                // if your partition, hit only self
                if (partition == self.getPartitionId()
                        && category == categoryFromCategoryId(self.getCategoryId())) {
                    trigger(new SearchMessage.Request(self.getAddress(), self.getAddress(),
                            event.getTimeoutId(), event.getTimeoutId(), event.getPattern(),
                            partition), networkPort);

                    continue;
                }

                TreeSet<SearchDescriptor> bucket = sortByConnectivity(categoryRoutingMap.get(partition));
                TreeSet<SearchDescriptor> unconnectedNodes = null;
                Iterator<SearchDescriptor> iterator = bucket.iterator();
                for (int i = 0; i < config.getSearchParallelism() && iterator.hasNext(); i++) {
                    SearchDescriptor searchDescriptor = iterator.next();

                    RTTStore.RTT rtt = RTTStore.getRtt(searchDescriptor.getId(), searchDescriptor.getVodAddress());
                    double latestRttsAvg = getLatestRttsAvg();
                    if (rtt != null && latestRttsAvg != 0 && rtt.getRttStats().getAvgRTT() > (config.getRttAnomalyTolerance() * latestRttsAvg)) {
                        if (unconnectedNodes == null) {
                            unconnectedNodes = getUnconnectedNodes(bucket);
                        }

                        if (!unconnectedNodes.isEmpty()) {
                            searchDescriptor = unconnectedNodes.pollFirst();
                        }
                    }

                    ScheduleTimeout scheduleTimeout = new ScheduleTimeout(event.getQueryTimeout());
                    scheduleTimeout.setTimeoutEvent(new SearchMessage.RequestTimeout(scheduleTimeout, self.getId(), searchDescriptor));
                    trigger(scheduleTimeout, timerPort);
                    trigger(new SearchMessage.Request(self.getAddress(), searchDescriptor.getVodAddress(),
                            scheduleTimeout.getTimeoutEvent().getTimeoutId(), event.getTimeoutId(), event.getPattern(),
                            partition), networkPort);

                    shuffleTimes.put(scheduleTimeout.getTimeoutEvent().getTimeoutId().getId(), System.currentTimeMillis());
                    searchDescriptor.setConnected(true);
                }
            }
        }
    };
    final Handler<SearchMessage.Response> handleSearchResponse = new Handler<SearchMessage.Response>() {
        @Override
        public void handle(SearchMessage.Response event) {

            // Search response is a UDT Message, so fix the ports before processing.
            TransportHelper.checkTransportAndUpdateBeforeReceiving(event);

            CancelTimeout cancelTimeout = new CancelTimeout(event.getTimeoutId());
            trigger(cancelTimeout, timerPort);

            Long timeStarted = shuffleTimes.remove(event.getTimeoutId().getId());
            if (timeStarted == null) {
                return;
            }
            long rtt = System.currentTimeMillis() - timeStarted;
            RTTStore.addSample(self.getId(), event.getVodSource(), rtt);
            updateLatestRtts(rtt);
        }
    };
    final Handler<SearchMessage.RequestTimeout> handleSearchRequestTimeout = new Handler<SearchMessage.RequestTimeout>() {
        @Override
        public void handle(SearchMessage.RequestTimeout event) {
            SearchDescriptor unresponsiveNode = event.getSearchDescriptor();

            shuffleTimes.remove(event.getTimeoutId().getId());
            publishUnresponsiveNode(unresponsiveNode.getVodAddress());
        }
    };
    /**
     * Handles broadcast public key request from Search component
     */
    final Handler<PublicKeyBroadcast> handlePublicKeyBroadcast = new Handler<PublicKeyBroadcast>() {
        @Override
        public void handle(PublicKeyBroadcast publicKeyBroadcast) {

            leaderPublicKey = publicKeyBroadcast.getPublicKey();
            //leaderAddress is used by a non leader node to directly send AddIndex request to leader. Since the
            //current node is now a leader, this information is not invalid.
            leaderAddress = null;
        }
    };
    /**
     * Responses with peer's view size
     */
    final Handler<ViewSizeMessage.Request> handleViewSizeRequest = new Handler<ViewSizeMessage.Request>() {
        @Override
        public void handle(ViewSizeMessage.Request request) {
            trigger(new ViewSizeMessage.Response(request.getTimeoutId(), request.getNewEntry(), gradientView.getSize(), request.getSource()), gradientRoutingPort);
        }
    };

    Handler<LeaderGroupInformation.Request> handleLeaderGroupInformationRequest = new Handler<LeaderGroupInformation.Request>() {
        @Override
        public void handle(LeaderGroupInformation.Request event) {

            logger.debug(" Partitioning Protocol Initiated at Leader." + self.getAddress().getId());
            int leaderGroupSize = event.getLeaderGroupSize();
            NavigableSet<SearchDescriptor> lowerUtilityNodes = ((NavigableSet)gradientView.getLowerUtilityNodes()).descendingSet();
            List<VodAddress> leaderGroupAddresses = new ArrayList<VodAddress>();

            // If gradient not full or not enough nodes in leader group.
            if((gradientView.getAll().size() < config.getViewSize())|| (lowerUtilityNodes.size() < leaderGroupSize)){
                trigger(new LeaderGroupInformation.Response(event.getMedianId(),event.getPartitioningType(), leaderGroupAddresses), gradientRoutingPort);
                return;
            }

            int i=0;
            for(SearchDescriptor desc : lowerUtilityNodes){

                if(i == leaderGroupSize)
                    break;
                leaderGroupAddresses.add(desc.getVodAddress());
                i++;
            }
            trigger(new LeaderGroupInformation.Response(event.getMedianId(), event.getPartitioningType(), leaderGroupAddresses), gradientRoutingPort);
        }
    };


    private boolean determineYourPartitionAndUpdatePartitionsNumberUpdated(VodAddress.PartitioningType partitionsNumber) {
        int nodeId = self.getId();

        PartitionId selfPartitionId = new PartitionId(partitionsNumber, self.getPartitionIdDepth(),
                self.getPartitionId());

        boolean partitionSubId = PartitionHelper.determineYourNewPartitionSubId(nodeId, selfPartitionId);

        if (partitionsNumber == VodAddress.PartitioningType.NEVER_BEFORE) {
            int partitionId = (partitionSubId ? 1 : 0);

            int selfCategory = self.getCategoryId();
            int newOverlayId = PartitionHelper.encodePartitionDataAndCategoryIdAsInt(VodAddress.PartitioningType.ONCE_BEFORE,
                    1, partitionId, selfCategory);

            // TODO - all existing VodAddresses in Sets, Maps, etc are now invalid.
            // Do we replace them or what do we do with them?
            ((MsSelfImpl) self).setOverlayId(newOverlayId);

        } else {
            int newPartitionId = self.getPartitionId() | ((partitionSubId ? 1 : 0) << self.getPartitionIdDepth());
            int selfCategory = self.getCategoryId();

            // Incrementing partitioning depth in the overlayId.
            int newOverlayId = PartitionHelper.encodePartitionDataAndCategoryIdAsInt(VodAddress.PartitioningType.MANY_BEFORE,
                    self.getPartitionIdDepth()+1, newPartitionId, selfCategory);
            ((MsSelfImpl) self).setOverlayId(newOverlayId);
        }
        logger.debug("Partitioning Occurred at Node: " + self.getId() + " PartitionDepth: " + self.getPartitionIdDepth() +" PartitionId: " + self.getPartitionId() + " PartitionType: " + self.getPartitioningType());
        int partitionId = self.getPartitionId();
        Snapshot.updateInfo(self.getAddress());                 // Overlay id present in the snapshot not getting updated, so added the method.
        Snapshot.addPartition(new Pair<Integer, Integer>(self.getCategoryId(), partitionId));
        return partitionSubId;
    }



    private MsConfig.Categories categoryFromCategoryId(int categoryId) {
        return MsConfig.Categories.values()[categoryId];
    }

    private TreeSet<SearchDescriptor> sortByConnectivity(Collection<SearchDescriptor> searchDescriptors) {
        // Need to sort it every time because values like MsSelfImpl.RTT might have been changed
        TreeSet<SearchDescriptor> sortedSearchDescriptors = new TreeSet<SearchDescriptor>(searchDescriptors);
        return sortedSearchDescriptors;
    }

    private TreeSet<SearchDescriptor> getUnconnectedNodes(Collection<SearchDescriptor> searchDescriptors) {
        TreeSet<SearchDescriptor> unconnectedNodes = new TreeSet<SearchDescriptor>(peerConnectivityComparator);
        for (SearchDescriptor searchDescriptor : searchDescriptors) {
            if (searchDescriptor.isConnected() == false) {
                unconnectedNodes.add(searchDescriptor);
            }
        }
        return unconnectedNodes;
    }

    private void updateLatestRtts(long rtt) {
        latestRtts[latestRttRingBufferPointer] = rtt;
        latestRttRingBufferPointer = (latestRttRingBufferPointer + 1) % config.getLatestRttStoreLimit();
    }

    private double getLatestRttsAvg() {
        long sum = 0;
        int numberOfSamples = 0;

        for (int i = 0; i < latestRtts.length; i++) {
            if (latestRtts[i] == 0) {
                break;
            }
            sum += latestRtts[i];
            numberOfSamples++;
        }

        if (numberOfSamples == 0) {
            return 0;
        }

        return sum / (double) numberOfSamples;
    }

    // If you call this method with a list of entries, it will
    // return a single node, weighted towards the 'best' node (as defined by
    // ComparatorById) with the temperature controlling the weighting.
    // QueryLimit temperature of '1.0' will be greedy and always return the best node.
    // QueryLimit temperature of '0.000001' will return a random node.
    // QueryLimit temperature of '0.0' will throw a divide by zero exception :)
    // Reference:
    // http://webdocs.cs.ualberta.ca/~sutton/book/2/node4.html
    private SearchDescriptor getSoftMaxAddress(List<SearchDescriptor> entries) {
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





     /**
     * Handler Apply Partitioning Updates.
     */
    Handler<GradientRoutingPort.ApplyPartitioningUpdate> handlePartitioningUpdate = new Handler<GradientRoutingPort.ApplyPartitioningUpdate>(){

        @Override
        public void handle(GradientRoutingPort.ApplyPartitioningUpdate event) {
            applyPartitioningUpdate(event.getPartitionUpdates());
        }
    };


    /**
     * Apply the partitioning updates received.
     */
    public void applyPartitioningUpdate(LinkedList<PartitionHelper.PartitionInfo> partitionUpdates){

        for(PartitionHelper.PartitionInfo update : partitionUpdates){

            boolean duplicateFound = false;
            for(PartitionHelper.PartitionInfo partitionInfo : partitionHistory){
                if(partitionInfo.getRequestId().getId() == update.getRequestId().getId()){
                    duplicateFound = true;
                    break;
                }
            }

            if(duplicateFound)
                continue;

            if(partitionHistory.size() >= HISTORY_LENGTH){
                partitionHistory.removeFirst();
            }

            // Store the update in the history.
            partitionHistory.addLast(update);

            // Now apply the update.
            // Leader boolean simply sends true down the message in case of leader node, as it was implemented like this way before, not sure why.
            boolean partition = determineYourPartitionAndUpdatePartitionsNumberUpdated(update.getPartitioningTypeInfo());
            gradientView.adjustViewToNewPartitions();

//            if(self.getId() == 1879934641){
//                logger.warn(" _Abhi: GOING TO PRINT MY GRADIENT AFTER PARTITION: ");
//                logger.warn("_Abhi: MyOverlayId" + self.getOverlayId());
//                for(SearchDescriptor desc : gradientView.getAll())
//                    logger.warn(" _Abhi: Descriptor: " + desc.getId());
//            }

            trigger(new LeaderStatusPort.TerminateBeingLeader(), leaderStatusPort);

            trigger(new RemoveEntriesNotFromYourPartition(partition, update.getMedianId()), gradientRoutingPort);
        }
    }


    // Control Message Exchange Code.


    /**
     * Received the command to initiate the pull based control message exchange mechanism.
     */
    Handler<GradientRoutingPort.InitiateControlMessageExchangeRound> handlerControlMessageExchangeInitiation = new Handler<GradientRoutingPort.InitiateControlMessageExchangeRound>() {
        @Override
        public void handle(GradientRoutingPort.InitiateControlMessageExchangeRound event) {

            ArrayList<SearchDescriptor> preferredNodes = new ArrayList<SearchDescriptor>(gradientView.getHigherUtilityNodes());

            // In case the higher utility nodes are less than the required ones, introduce the lower utility nodes also.
            if(preferredNodes.size() < event.getControlMessageExchangeNumber())
                preferredNodes.addAll(gradientView.getLowerUtilityNodes());

            // NOTE: Now if the node size is less than required, then return.
            if(preferredNodes.size() < event.getControlMessageExchangeNumber())
                return;

            List<Integer> randomIntegerList = getUniqueRandomIntegerList(preferredNodes.size(), event.getControlMessageExchangeNumber());
            for(int n : randomIntegerList){
                VodAddress destination = preferredNodes.get(n).getVodAddress();
//                if(self.getId() == 319791623)
//                    logger.warn("_CASE: Sending to : " + destination.getId());
                trigger(new ControlMessage.Request(self.getAddress(), destination, new OverlayId(self.getOverlayId()), event.getRoundId()), networkPort);
            }
        }
    };


    /**
     * Based on the parameters passed, it returns a random set of elements.
     * @param sizeOfAvailableObjectSet
     * @param randomSetSize
     * @return
     */
    public List<Integer> getUniqueRandomIntegerList(int sizeOfAvailableObjectSet, int randomSetSize){

        //Create an instance of random integer list.
        List<Integer> uniqueRandomIntegerList = new ArrayList<Integer>();

        // In case any size is <=0 just return empty list.
        if(sizeOfAvailableObjectSet <=0 || randomSetSize <=0){
            return uniqueRandomIntegerList;
        }

        // Can't return random element positions in case the size is lower than required.
        if(sizeOfAvailableObjectSet < randomSetSize){
            for(int i =0 ; i < sizeOfAvailableObjectSet ; i ++){
                uniqueRandomIntegerList.add(i);
            }
        }
        else{

            while(uniqueRandomIntegerList.size() < randomSetSize){

                int n = random.nextInt(sizeOfAvailableObjectSet);
                if(!uniqueRandomIntegerList.contains(n))
                    uniqueRandomIntegerList.add(n);
            }
        }

        return uniqueRandomIntegerList;
    }


    Handler<ControlMessageInternal.Request> handlerControlMessageInternalRequest = new Handler<ControlMessageInternal.Request>(){
        @Override
        public void handle(ControlMessageInternal.Request event) {

            if(event instanceof  CheckPartitionInfoHashUpdate.Request)
                handleCheckPartitionInternalControlMessage((CheckPartitionInfoHashUpdate.Request)event);
            else if(event instanceof  CheckLeaderInfoUpdate.Request)
                handleCheckLeaderInfoInternalControlMessage((CheckLeaderInfoUpdate.Request) event);
        }
    };

    private void handleCheckLeaderInfoInternalControlMessage(CheckLeaderInfoUpdate.Request event) {

        // Check for the partitioning updates and return it back.
        logger.debug("Check Leader Update Received.");

        trigger(new CheckLeaderInfoUpdate.Response(event.getRoundId(), event.getSourceAddress(),
                leader ? self.getAddress() : leaderAddress, leaderPublicKey), gradientRoutingPort);
    }

    Handler<LeaderInfoUpdate> handleLeaderUpdate = new Handler<LeaderInfoUpdate>() {
        @Override
        public void handle(LeaderInfoUpdate leaderInfoUpdate) {

            leaderAddress = leaderInfoUpdate.getLeaderAddress();
            leaderPublicKey = leaderInfoUpdate.getLeaderPublicKey();
        }
    };

    /**
     * Request To check if the source address is behind in terms of partitioning updates.
     */
    private void handleCheckPartitionInternalControlMessage(CheckPartitionInfoHashUpdate.Request event) {

        // Check for the partitioning updates and return it back.
        logger.debug("Check Partitioning Update Received.");

        LinkedList<PartitionHelper.PartitionInfoHash> partitionUpdateHashes = new LinkedList<PartitionHelper.PartitionInfoHash>();
        ControlMessageEnum controlMessageEnum;

        // Check for the responses when you have atleast partitioned yourself.
        if (self.getPartitioningType() != VodAddress.PartitioningType.NEVER_BEFORE) {

            controlMessageEnum = fetchPartitioningHashUpdatesMessageEnum(event.getSourceAddress(), event.getOverlayId(), partitionUpdateHashes);
        } else {
            // Send empty partition update as you have not partitioned.
            controlMessageEnum = ControlMessageEnum.PARTITION_UPDATE;
        }

        // Trigger the partitioning update.
        trigger(new CheckPartitionInfoHashUpdate.Response(event.getRoundId(), event.getSourceAddress(), partitionUpdateHashes, controlMessageEnum), gradientRoutingPort);
    }


    /**
     * Based on the source address, provide the control message enum that needs to be associated with the control response object.
     */
    private ControlMessageEnum fetchPartitioningHashUpdatesMessageEnum(VodAddress address, OverlayId overlayId, List<PartitionHelper.PartitionInfoHash> partitionUpdateHashes){

        boolean isOnePartition = self.getPartitioningType() == VodAddress.PartitioningType.ONCE_BEFORE;

        // for ONE_BEFORE
        if(isOnePartition){
            if(overlayId.getPartitioningType() == VodAddress.PartitioningType.NEVER_BEFORE){
                for(PartitionHelper.PartitionInfo partitionInfo: partitionHistory)
                    partitionUpdateHashes.add(new PartitionHelper.PartitionInfoHash(partitionInfo));
            }
        }

        // for MANY_BEFORE.
        else {

            int myDepth = self.getPartitionIdDepth();
            if (overlayId.getPartitioningType() == VodAddress.PartitioningType.NEVER_BEFORE) {

                if (myDepth <= (HISTORY_LENGTH)) {
                    for(PartitionHelper.PartitionInfo partitionInfo: partitionHistory)
                        partitionUpdateHashes.add(new PartitionHelper.PartitionInfoHash(partitionInfo));
                }
                else
                    return ControlMessageEnum.REJOIN;
            }
            else {

                int receivedNodeDepth = overlayId.getPartitionIdDepth();
                if(myDepth - receivedNodeDepth > HISTORY_LENGTH)
                    return ControlMessageEnum.REJOIN;

                else if ((myDepth - receivedNodeDepth) <= (HISTORY_LENGTH) && (myDepth - receivedNodeDepth) > 0) {

                    // TODO : Test this condition.
                    int j = partitionHistory.size() - (myDepth-receivedNodeDepth);
                    for(int i = 0 ; i < (myDepth-receivedNodeDepth) && j < HISTORY_LENGTH ; i++){
                        partitionUpdateHashes.add(new PartitionHelper.PartitionInfoHash(partitionHistory.get(j)));
                        j++;
                    }
                }
            }
        }
        return ControlMessageEnum.PARTITION_UPDATE;
    }


    /**
     * Based on the supplied request Ids fetch the partitioning updates.
     */
    Handler<CheckPartitionInfo.Request> handlerCheckPartitioningInfoRequest = new Handler<CheckPartitionInfo.Request>(){

        @Override
        public void handle(CheckPartitionInfo.Request event) {
            LinkedList<PartitionHelper.PartitionInfo> partitionUpdates = fetchPartitioningUpdates(event.getPartitionUpdateIds());
            trigger(new CheckPartitionInfo.Response(event.getRoundId(),event.getSourceAddress(), partitionUpdates), gradientRoutingPort);
        }
    };


    /**
     * Based on the unique ids return the partition updates back.
     * @param partitionUpdatesIds
     * @return
     */
    public LinkedList<PartitionHelper.PartitionInfo> fetchPartitioningUpdates(List<TimeoutId> partitionUpdatesIds){

        LinkedList<PartitionHelper.PartitionInfo> partitionUpdates = new LinkedList<PartitionHelper.PartitionInfo>();

        // Iterate over the available partition updates.
        for(TimeoutId partitionUpdateId : partitionUpdatesIds){

            boolean found = false;
            for(PartitionHelper.PartitionInfo partitionInfo : partitionHistory){
                if(partitionInfo.getRequestId().equals(partitionUpdateId)){
                    partitionUpdates.add(partitionInfo);
                    found = true;
                    break;
                }
            }

            if(!found){
                // Stop The addition as there has been a missing piece.
                break;
            }
        }

        // Return the ordered update list.
        return partitionUpdates;
    }

}
