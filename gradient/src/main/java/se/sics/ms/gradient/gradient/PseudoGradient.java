package se.sics.ms.gradient.gradient;

import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.co.FailureDetectorPort;
import se.sics.gvod.common.RTTStore;
import se.sics.gvod.common.net.RttStats;
import se.sics.gvod.config.GradientConfiguration;
import se.sics.gvod.croupier.Croupier;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.VodNetwork;
import se.sics.gvod.timer.*;
import se.sics.gvod.timer.Timer;
import se.sics.gvod.timer.UUID;
import se.sics.kompics.*;
import se.sics.ms.aggregator.port.StatusAggregatorPort;
import se.sics.ms.common.MsSelfImpl;
import se.sics.ms.common.TransportHelper;
import se.sics.ms.configuration.MsConfig;
import se.sics.ms.gradient.control.CheckLeaderInfoUpdate;
import se.sics.ms.gradient.control.ControlMessageInternal;
import se.sics.ms.gradient.events.*;
import se.sics.ms.gradient.misc.SimpleUtilityComparator;
import se.sics.ms.gradient.ports.GradientRoutingPort;
import se.sics.ms.gradient.ports.GradientViewChangePort;
import se.sics.ms.gradient.ports.LeaderStatusPort;
import se.sics.ms.gradient.ports.LeaderStatusPort.LeaderStatus;
import se.sics.ms.gradient.ports.LeaderStatusPort.NodeCrashEvent;
import se.sics.ms.gradient.ports.PublicKeyPort;
import se.sics.ms.messages.*;
import se.sics.ms.ports.SelfChangedPort;
import se.sics.ms.types.*;
import se.sics.ms.types.OverlayId;

import java.security.PublicKey;
import java.util.*;

import static se.sics.ms.util.PartitionHelper.getPartitionIdOtherHalf;
import static se.sics.ms.util.PartitionHelper.removeOldBuckets;
import static se.sics.ms.util.PartitionHelper.updateBucketsInRoutingTable;

import se.sics.ms.util.PartitionHelper;
import se.sics.p2ptoolbox.croupier.api.CroupierPort;
import se.sics.p2ptoolbox.croupier.api.msg.CroupierSample;
import se.sics.p2ptoolbox.croupier.api.util.CroupierPeerView;
import se.sics.p2ptoolbox.croupier.api.util.PeerView;
import se.sics.p2ptoolbox.gradient.api.GradientPort;
import se.sics.p2ptoolbox.gradient.api.msg.GradientSample;

/**
 * The class is simply a wrapper over the Gradient service.
 * It handles all the tasks provided to it by other components.
 * <p/>
 * NOTE: Proper division of functionality is required. The class will soon be deprecated,
 * once the functionality move to there respective components.
 */
public final class PseudoGradient extends ComponentDefinition {

    private static final Logger logger = LoggerFactory.getLogger(PseudoGradient.class);
    Positive<VodNetwork> networkPort = positive(VodNetwork.class);
    Positive<Timer> timerPort = positive(Timer.class);

    Positive<GradientViewChangePort> gradientViewChangePort = positive(GradientViewChangePort.class);
    Positive<FailureDetectorPort> fdPort = requires(FailureDetectorPort.class);
    Negative<LeaderStatusPort> leaderStatusPort = negative(LeaderStatusPort.class);

    Positive<PublicKeyPort> publicKeyPort = positive(PublicKeyPort.class);
    Negative<GradientRoutingPort> gradientRoutingPort = negative(GradientRoutingPort.class);
    Positive<SelfChangedPort> selfChangedPort = positive(SelfChangedPort.class);

    Positive<StatusAggregatorPort> statusAggregatorPortPositive = positive(StatusAggregatorPort.class);
    Positive<GradientPort> gradientPort = positive(GradientPort.class);
    Positive<CroupierPort> croupierPort = positive(CroupierPort.class);

    private MsSelfImpl self;
    private GradientConfiguration config;
    private Random random;


    private boolean leader;
    private VodAddress leaderAddress;
    private PublicKey leaderPublicKey;

    private Map<Integer, Long> shuffleTimes = new HashMap<Integer, Long>();
    int latestRttRingBufferPointer = 0;
    private long[] latestRtts;
    String compName;

    private Map<MsConfig.Categories, Map<Integer, HashSet<SearchDescriptor>>> routingTable;
    private TreeSet<SearchDescriptor> gradientEntrySet;
    private SimpleUtilityComparator utilityComparator;

    private boolean converged;
    private boolean changed;

    private double convergenceTest;
    private int convergenceTestRounds;
    private int currentConvergedRounds;

    private IndexEntry indexEntryToAdd;
    private TimeoutId addIndexEntryRequestTimeoutId;
    final private HashSet<SearchDescriptor> queriedNodes = new HashSet<SearchDescriptor>();

    final private HashMap<TimeoutId, SearchDescriptor> openRequests = new HashMap<TimeoutId, SearchDescriptor>();
    final private HashMap<VodAddress, Integer> locatedLeaders = new HashMap<VodAddress, Integer>();
    private List<VodAddress> leadersAlreadyComunicated = new ArrayList<VodAddress>();


    // Routing Table Update Information.
    private Map<MsConfig.Categories, Map<Integer, Pair<Integer, HashSet<SearchDescriptor>>>> routingTableUpdated;



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


    public PseudoGradient(PseudoGradientInit init) {

        doInit(init);
        subscribe(handleStart, control);
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

        subscribe(handleLeaderGroupInformationRequest, gradientRoutingPort);
        subscribe(handleFailureDetector, fdPort);
        subscribe(handlerControlMessageExchangeInitiation, gradientRoutingPort);

        subscribe(handlerControlMessageInternalRequest, gradientRoutingPort);
        subscribe(handlerSelfChanged, selfChangedPort);
        subscribe(gradientSampleHandler, gradientPort);
        subscribe(croupierSampleHandler, croupierPort);

    }

    /**
     * Initialize the state of the component.
     */
    private void doInit(PseudoGradientInit init) {

        self = ((MsSelfImpl) init.getSelf()).clone();
        config = init.getConfiguration();
        random = new Random(init.getConfiguration().getSeed());

        routingTable = new HashMap<MsConfig.Categories, Map<Integer, HashSet<SearchDescriptor>>>();
        leader = false;
        leaderAddress = null;
        latestRtts = new long[config.getLatestRttStoreLimit()];

        compName = "(" + self.getId() + ", " + self.getOverlayId() + ") ";
        utilityComparator = new SimpleUtilityComparator();
        gradientEntrySet = new TreeSet<SearchDescriptor>(utilityComparator);

        this.converged = false;
        this.changed = false;
        this.convergenceTest = config.getConvergenceTest();
        this.convergenceTestRounds = config.getConvergenceTestRounds();

        this.routingTableUpdated = new HashMap<MsConfig.Categories, Map<Integer, Pair<Integer, HashSet<SearchDescriptor>>>>();
    }

    public Handler<Start> handleStart = new Handler<Start>() {

        @Override
        public void handle(Start e) {
            logger.info("Pseudo Gradient Component Started ...");
        }
    };

    private void removeNodeFromRoutingTable(OverlayAddress nodeToRemove) {
        MsConfig.Categories category = categoryFromCategoryId(nodeToRemove.getCategoryId());
        Map<Integer, HashSet<SearchDescriptor>> categoryRoutingMap = routingTable.get(category);
        if (categoryRoutingMap != null) {
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

        for (VodAddress suspectedNode : nodesToRemove) {
            removeNodeFromLocalState(new OverlayAddress(suspectedNode));
        }
    }

    private void removeNodeFromLocalState(OverlayAddress overlayAddress) {
        removeNodeFromRoutingTable(overlayAddress);
        RTTStore.removeSamples(overlayAddress.getId(), overlayAddress.getAddress());
    }

    private void publishUnresponsiveNode(VodAddress nodeAddress) {
        trigger(new FailureDetectorPort.FailureDetectorEvent(nodeAddress), fdPort);
    }

    final Handler<FailureDetectorPort.FailureDetectorEvent> handleFailureDetector = new Handler<FailureDetectorPort.FailureDetectorEvent>() {

        @Override
        public void handle(FailureDetectorPort.FailureDetectorEvent event) {
            removeNodesFromLocalState(event.getSuspectedNodes());
        }
    };

    /**
     * Broadcast the current view to the listening components.
     */

    void sendGradientViewChange() {

        if (isChanged()) {
            // Create a copy so components don't affect each other
            SortedSet<SearchDescriptor> view = new TreeSet<SearchDescriptor>(getGradientSample());
            trigger(new GradientViewChangePort.GradientViewChanged(isConverged(), view), gradientViewChangePort);
        }
    }


    /**
     * Helper Method to test the instance type of entries in a list.
     * If match is found, then process the entry by adding to result list.
     *
     * @param baseList   List to append entries to.
     * @param sampleList List to iterate over.
     */
    private void checkInstanceAndAdd(Collection<SearchDescriptor> baseList, Collection<CroupierPeerView> sampleList) {

        for (CroupierPeerView croupierPeerView : sampleList) {

            if (croupierPeerView.pv instanceof SearchDescriptor) {
                SearchDescriptor currentDescriptor = (SearchDescriptor) croupierPeerView.pv;
                currentDescriptor.setAge(croupierPeerView.getAge());
                baseList.add(currentDescriptor);
            }
        }
    }


    /**
     * On each of merge of the random samples,
     * increment the age of routing table entries.
     */
    private void incrementRoutingTableAge() {
        for (Map<Integer, HashSet<SearchDescriptor>> categoryRoutingMap : routingTable.values()) {
            for (HashSet<SearchDescriptor> bucket : categoryRoutingMap.values()) {
                for (SearchDescriptor descriptor : bucket) {
                    descriptor.incrementAndGetAge();
                }
            }
        }
    }

    /**
     * Based on the collection of the peers supplied, update the current routing table.
     * A mechanism to clean the routing table by removing the old entries.
     *
     * @param nodes collection of peers
     */
    private void addRoutingTableEntries(Collection<SearchDescriptor> nodes) {

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

                PartitionId newPartitionId = new PartitionId(searchDescriptor.getOverlayAddress().getPartitioningType(),
                        searchDescriptor.getOverlayAddress().getPartitionIdDepth(), searchDescriptor.getOverlayAddress().getPartitionId());
                updateBucketsInRoutingTable(newPartitionId, categoryRoutingMap, bucket);
            }

            bucket.add(searchDescriptor);
            TreeSet<SearchDescriptor> sortedBucket = sortByConnectivity(bucket);
            while (bucket.size() > config.getMaxNumRoutingEntries()) {
                bucket.remove(sortedBucket.pollLast());
            }
        }
    }


    /**
     * Based on the provided collection of the nodes, update the routing table information.
     *
     * @param nodes Collection of nodes.
     */
    private void addRoutingTableEntriesUpdated (Collection<SearchDescriptor> nodes){

        for (SearchDescriptor descriptor : nodes){

            MsConfig.Categories category = categoryFromCategoryId(descriptor.getOverlayId().getCategoryId());

            PartitionId partitionInfo = new PartitionId(descriptor.getOverlayAddress().getPartitioningType(),
                    descriptor.getOverlayAddress().getPartitionIdDepth(), descriptor.getOverlayAddress().getPartitionId());

            Map<Integer, Pair<Integer, HashSet<SearchDescriptor>>> categoryRoutingMap = routingTableUpdated.get(category);

            if(categoryRoutingMap == null){
                categoryRoutingMap = new HashMap<Integer, Pair<Integer, HashSet<SearchDescriptor>>>();
                routingTableUpdated.put(category, categoryRoutingMap);
            }
            Pair<Integer, HashSet<SearchDescriptor>> partitionBucket = categoryRoutingMap.get(partitionInfo.getPartitionId());

            if(partitionBucket == null){
                checkAndAddNewBucket(partitionInfo, categoryRoutingMap);
                continue;
            }

            int comparisonResult = new Integer(partitionInfo.getPartitionIdDepth()).compareTo(partitionBucket.getValue0());
            if(comparisonResult > 0){
                logger.debug("Need to remove the old bucket and create own");
                checkAndAddNewBucket(partitionInfo, categoryRoutingMap);
            }

            else if(comparisonResult < 0){
                logger.debug("Received a node with lower partition depth, not incorporating it.");
            }

            else{
                logger.debug("Add node and then remove the weakest node in terms of age or RTT.");
                partitionBucket.getValue1().add(descriptor);
                logger.warn("Need to check for the size and remove the nodes with oldest age or RTT.");
            }

        }
    }


    /**
     * Based on the partition and routing map information, check for old  buckets, clean them up and add new buckets with updated
     * partition information.
     * @param partitionInfo
     * @param categoryRoutingMap
     */
    private void checkAndAddNewBucket(PartitionId partitionInfo, Map<Integer, Pair<Integer, HashSet<SearchDescriptor>>> categoryRoutingMap){

        Pair<Integer, HashSet<SearchDescriptor>> newPartitionBucket = Pair.with( partitionInfo.getPartitionIdDepth(), new HashSet<SearchDescriptor>());
        removeOldBuckets(partitionInfo, categoryRoutingMap);
        categoryRoutingMap.put(partitionInfo.getPartitionId(), newPartitionBucket);

        int otherPartitionId = PartitionHelper.getPartitionIdOtherHalf(partitionInfo);
        Pair<Integer, HashSet<SearchDescriptor>> otherPartitionBucket = Pair.with(partitionInfo.getPartitionIdDepth(), new HashSet<SearchDescriptor>());
        categoryRoutingMap.put(otherPartitionId, otherPartitionBucket);
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

            if (addCategory == selfCategory) {
                if (leader) {
                    trigger(new AddIndexEntryMessage.Request(self.getAddress(), self.getAddress(), event.getTimeoutId(), event.getEntry()), networkPort);
                }

                //if we have direct pointer to the leader
                else if (leaderAddress != null) {
                    //sendLeaderLookupRequest(new SearchDescriptor(leaderAddress));
                    trigger(new AddIndexEntryMessage.Request(self.getAddress(), leaderAddress, event.getTimeoutId(), event.getEntry()), networkPort);
                } else {
                    NavigableSet<SearchDescriptor> startNodes = new TreeSet<SearchDescriptor>(utilityComparator);
                    startNodes.addAll(getGradientSample());

                    Map<Integer, HashSet<SearchDescriptor>> croupierPartitions = routingTable.get(selfCategory);
                    if (croupierPartitions != null && !croupierPartitions.isEmpty()) {
                        HashSet<SearchDescriptor> croupierNodes = croupierPartitions.get(self.getPartitionId());
                        if (croupierNodes != null && !croupierNodes.isEmpty()) {
                            startNodes.addAll(croupierNodes);
                        }
                    }

                    Iterator<SearchDescriptor> iterator = startNodes.descendingIterator();

                    for (int i = 0; i < LeaderLookupMessage.QueryLimit && iterator.hasNext(); i++) {
                        SearchDescriptor node = iterator.next();
                        sendLeaderLookupRequest(node);
                    }
                }
            } else {
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

            TreeSet<SearchDescriptor> higherNodes = new TreeSet<SearchDescriptor>(getHigherUtilityNodes());
            ArrayList<SearchDescriptor> searchDescriptors = new ArrayList<SearchDescriptor>();

            Iterator<SearchDescriptor> iterator = higherNodes.descendingIterator();
            while (searchDescriptors.size() < LeaderLookupMessage.ResponseLimit && iterator.hasNext()) {
                searchDescriptors.add(iterator.next());
            }

            if (searchDescriptors.size() < LeaderLookupMessage.ResponseLimit) {

                TreeSet<SearchDescriptor> lowerNodes = new TreeSet<SearchDescriptor>(getLowerUtilityNodes());
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
                    Collections.reverse(higherUtilityNodes);
                }

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

                    if (!leadersAlreadyComunicated.contains(locatedLeader)) {
                        trigger(new AddIndexEntryMessage.Request(self.getAddress(), locatedLeader, addIndexEntryRequestTimeoutId, indexEntryToAdd), networkPort);
                        leadersAlreadyComunicated.add(locatedLeader);
                    }
                }
            }
        }
    };

    /**
     * Relaying of the look up request. I am not leader or doesn't know anyone therefore I route the lookup request,
     * higher in the gradient in hope of other nodes knowing the information.
     *
     * @param node Peer
     */
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

            for (SearchDescriptor peer : getLowerUtilityNodes()) {
                trigger(new ReplicationPrepareCommitMessage.Request(self.getAddress(), peer.getVodAddress(), event.getTimeoutId(), event.getEntry()), networkPort);
            }
        }
    };

    final Handler<GradientRoutingPort.ReplicationCommit> handleReplicationCommit = new Handler<GradientRoutingPort.ReplicationCommit>() {
        @Override
        public void handle(GradientRoutingPort.ReplicationCommit event) {

            for (SearchDescriptor peer : getLowerUtilityNodes()) {
                trigger(new ReplicationCommitMessage.Request(self.getAddress(), peer.getVodAddress(), event.getTimeoutId(), event.getIndexEntryId(), event.getSignature()), networkPort);
            }

        }
    };

    /**
     * Index Exchange mechanism requires the information of the higher utility nodes,
     * which have high chances of having the data as they are already above in the gradient.
     */
    final Handler<GradientRoutingPort.IndexHashExchangeRequest> handleIndexHashExchangeRequest = new Handler<GradientRoutingPort.IndexHashExchangeRequest>() {
        @Override
        public void handle(GradientRoutingPort.IndexHashExchangeRequest event) {

            ArrayList<SearchDescriptor> nodes = new ArrayList<SearchDescriptor>(getHigherUtilityNodes());
            if (nodes.isEmpty() || nodes.size() < event.getNumberOfRequests()) {
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

    /**
     * During searching for text, request sent to look into the routing table and
     * fetch the nodes from the neighbouring partitions and also from other categories.
     */
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
            // NOTE: LeaderAddress is used by a non leader node to directly send AddIndex request to leader.
            // Since the current node is now a leader, this information is not invalid.
            leaderPublicKey = publicKeyBroadcast.getPublicKey();
            leaderAddress = null;
        }
    };

    /**
     * Responses with peer's view size
     */
    final Handler<ViewSizeMessage.Request> handleViewSizeRequest = new Handler<ViewSizeMessage.Request>() {
        @Override
        public void handle(ViewSizeMessage.Request request) {
            trigger(new ViewSizeMessage.Response(request.getTimeoutId(), request.getNewEntry(), getGradientSample().size(), request.getSource()), gradientRoutingPort);
        }
    };


    /**
     * Request Received to provide other component with information regarding the nodes
     * neighbouring the leader.
     */
    Handler<LeaderGroupInformation.Request> handleLeaderGroupInformationRequest = new Handler<LeaderGroupInformation.Request>() {
        @Override
        public void handle(LeaderGroupInformation.Request event) {

            logger.debug(" Partitioning Protocol Initiated at Leader." + self.getAddress().getId());
            int leaderGroupSize = event.getLeaderGroupSize();
            NavigableSet<SearchDescriptor> lowerUtilityNodes = ((NavigableSet) getLowerUtilityNodes()).descendingSet();
            List<VodAddress> leaderGroupAddresses = new ArrayList<VodAddress>();

            if ((getGradientSample().size() < config.getViewSize()) || (lowerUtilityNodes.size() < leaderGroupSize)) {
                trigger(new LeaderGroupInformation.Response(event.getMedianId(), event.getPartitioningType(), leaderGroupAddresses), gradientRoutingPort);
                return;
            }


            int i = 0;
            for (SearchDescriptor desc : lowerUtilityNodes) {

                if (i == leaderGroupSize)
                    break;
                leaderGroupAddresses.add(desc.getVodAddress());
                i++;
            }
            trigger(new LeaderGroupInformation.Response(event.getMedianId(), event.getPartitioningType(), leaderGroupAddresses), gradientRoutingPort);
        }
    };


    private MsConfig.Categories categoryFromCategoryId(int categoryId) {
        return MsConfig.Categories.values()[categoryId];
    }

    /**
     * Need to sort it every time because values like MsSelfImpl.RTT might have been changed
     *
     * @param searchDescriptors Descriptors
     * @return Sorted Set.
     */
    private TreeSet<SearchDescriptor> sortByConnectivity(Collection<SearchDescriptor> searchDescriptors) {
        return new TreeSet<SearchDescriptor>(searchDescriptors);
    }

    /**
     * Fetch the nodes that are unconnected in the system.
     *
     * @param searchDescriptors Search Descriptor Collection
     * @return Unconnected Nodes
     */
    private TreeSet<SearchDescriptor> getUnconnectedNodes(Collection<SearchDescriptor> searchDescriptors) {
        TreeSet<SearchDescriptor> unconnectedNodes = new TreeSet<SearchDescriptor>(peerConnectivityComparator);
        for (SearchDescriptor searchDescriptor : searchDescriptors) {
            if (!searchDescriptor.isConnected()) {
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

    // Control Message Exchange Code.


    /**
     * Received the command to initiate the pull based control message exchange mechanism.
     */
    Handler<GradientRoutingPort.InitiateControlMessageExchangeRound> handlerControlMessageExchangeInitiation = new Handler<GradientRoutingPort.InitiateControlMessageExchangeRound>() {
        @Override
        public void handle(GradientRoutingPort.InitiateControlMessageExchangeRound event) {

            ArrayList<SearchDescriptor> preferredNodes = new ArrayList<SearchDescriptor>(getHigherUtilityNodes());

            if (preferredNodes.size() < event.getControlMessageExchangeNumber())
                preferredNodes.addAll(getLowerUtilityNodes());

            if (preferredNodes.size() < event.getControlMessageExchangeNumber())
                return;

            for (int i = 0; i < event.getControlMessageExchangeNumber(); i++) {
                VodAddress destination = preferredNodes.get(i).getVodAddress();
                trigger(new ControlMessage.Request(self.getAddress(), destination, new OverlayId(self.getOverlayId()), event.getRoundId()), networkPort);
            }
        }
    };


    Handler<LeaderInfoUpdate> handleLeaderUpdate = new Handler<LeaderInfoUpdate>() {
        @Override
        public void handle(LeaderInfoUpdate leaderInfoUpdate) {
            leaderAddress = leaderInfoUpdate.getLeaderAddress();
            leaderPublicKey = leaderInfoUpdate.getLeaderPublicKey();
        }
    };

    Handler<ControlMessageInternal.Request> handlerControlMessageInternalRequest = new Handler<ControlMessageInternal.Request>() {
        @Override
        public void handle(ControlMessageInternal.Request event) {

            if (event instanceof CheckLeaderInfoUpdate.Request)
                handleCheckLeaderInfoInternalControlMessage((CheckLeaderInfoUpdate.Request) event);
        }
    };

    private void handleCheckLeaderInfoInternalControlMessage(CheckLeaderInfoUpdate.Request event) {

        logger.debug("Check Leader Update Received.");

        trigger(new CheckLeaderInfoUpdate.Response(event.getRoundId(), event.getSourceAddress(),
                leader ? self.getAddress() : leaderAddress, leaderPublicKey), gradientRoutingPort);
    }

    Handler<SelfChangedPort.SelfChangedEvent> handlerSelfChanged = new Handler<SelfChangedPort.SelfChangedEvent>() {
        @Override
        public void handle(SelfChangedPort.SelfChangedEvent event) {
            self = event.getSelf().clone();
        }
    };


    /**
     * Croupier used to supply information regarding the <b>nodes in other partitions</b>,
     * incorporate the sample in the <b>routing table</b>.
     */
    Handler<CroupierSample> croupierSampleHandler = new Handler<CroupierSample>() {
        @Override
        public void handle(CroupierSample event) {
            logger.info("{}: Pseudo Gradient Received Croupier Sample", self.getId());
            Collection<SearchDescriptor> filteredCroupierSample = new ArrayList<SearchDescriptor>();

            if (event.publicSample.isEmpty())
                logger.info("{}: Pseudo Gradient Received Empty Sample: " + self.getId());

            Collection<CroupierPeerView> rawCroupierSample = new ArrayList<CroupierPeerView>();
            rawCroupierSample.addAll(event.publicSample);
            rawCroupierSample.addAll(event.privateSample);

            checkInstanceAndAdd(filteredCroupierSample, rawCroupierSample);
            addRoutingTableEntries(filteredCroupierSample);
            incrementRoutingTableAge();
        }
    };


    Handler<GradientSample> gradientSampleHandler = new Handler<GradientSample>() {
        @Override
        public void handle(GradientSample event) {

            logger.debug("{}: Received gradient sample", self.getId());
            Collection<SearchDescriptor> oldGradientEntrySet = (Collection<SearchDescriptor>) gradientEntrySet.clone();

            gradientEntrySet.clear();
            checkInstanceAndAdd(gradientEntrySet, event.gradientSample);
            performAdditionalHouseKeepingTasks(oldGradientEntrySet);

        }
    };


    /**
     * After every sample merge, perform some additional tasks
     * in a <b>predefined order</b>.
     *
     * @param oldGradientEntrySet changed gradient set
     */
    private void performAdditionalHouseKeepingTasks(Collection<SearchDescriptor> oldGradientEntrySet) {

        checkConvergence(oldGradientEntrySet, gradientEntrySet);
        sendGradientViewChange();
        publishSample();

    }


    private void publishSample() {

        Set<SearchDescriptor> nodes = getGradientSample();
        StringBuilder sb = new StringBuilder("Neighbours: { ");
        for (SearchDescriptor d : nodes) {
            sb.append(d.getVodAddress().getId() + ":" + d.getNumberOfIndexEntries() + ":" + d.getReceivedPartitionDepth() + ":" + d.getAge()).append(", ");

        }
        sb.append("}");
        logger.warn(compName + sb);
    }

    /**
     * Fetch the current gradient sample.
     *
     * @return Most Recent Gradient Sample.
     */
    private SortedSet<SearchDescriptor> getGradientSample() {
        return gradientEntrySet;
    }


    /**
     * Based on the changed gradient set, check the local convergence of the gradient.
     *
     * @param oldGradientEntrySet Old Entry Set
     * @param gradientEntrySet    Current Entry Set
     */
    private void checkConvergence(Collection<SearchDescriptor> oldGradientEntrySet, Collection<SearchDescriptor> gradientEntrySet) {

        int oldSize = oldGradientEntrySet.size();
        int newSize = gradientEntrySet.size();


        oldGradientEntrySet.retainAll(gradientEntrySet);

        if (oldSize == newSize && oldSize > convergenceTest * newSize) {
            currentConvergedRounds++;
        } else {
            currentConvergedRounds = 0;
        }

        if (currentConvergedRounds > convergenceTestRounds) {
            if (!converged) {
                this.changed = true;
            }
            converged = true;
        } else {
            converged = false;
        }
    }

    /**
     * Get the nodes which have the higher utility from the node.
     *
     * @return The Sorted Set.
     */
    private SortedSet<SearchDescriptor> getHigherUtilityNodes() {
        return gradientEntrySet.tailSet(new SearchDescriptor(self.getDescriptor()));
    }

    /**
     * Get the nodes which have lower utility as compared to node.
     *
     * @return Lower Utility Nodes.
     */
    private SortedSet<SearchDescriptor> getLowerUtilityNodes() {
        return gradientEntrySet.headSet(new SearchDescriptor(self.getDescriptor()));
    }

    /**
     * Has the node converged i.e the change within the gradient
     * is within the specified limits.
     *
     * @return local convergence
     */
    private boolean isConverged() {
        return converged;
    }


    /**
     * Has gradient sample changed within successive iterations.
     *
     * @return gradient change
     */
    private boolean isChanged() {
        return changed;
    }
}
