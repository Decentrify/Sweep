package se.sics.ms.gradient;

//import com.sun.xml.internal.bind.v2.TODO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.co.FailureDetectorPort;
import se.sics.gvod.common.RTTStore;
import se.sics.gvod.common.Self;
import se.sics.gvod.common.VodDescriptor;
import se.sics.gvod.common.net.RttStats;
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
import se.sics.ms.common.MsSelfImpl;
import se.sics.ms.configuration.MsConfig;
import se.sics.ms.gradient.LeaderStatusPort.LeaderStatus;
import se.sics.ms.gradient.LeaderStatusPort.NodeCrashEvent;
import se.sics.ms.messages.*;
import se.sics.ms.snapshot.Snapshot;
import se.sics.ms.timeout.IndividualTimeout;
import se.sics.ms.types.IndexEntry;
import se.sics.ms.types.PartitionId;
import se.sics.ms.util.Pair;
import se.sics.ms.util.PartitionHelper;

import java.security.PublicKey;
import java.util.*;

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
    private Self self;
    private GradientConfiguration config;
    private Random random;
    private GradientView gradientView;
    private UtilityComparator utilityComparator = new UtilityComparator();
    private Map<UUID, VodAddress> outstandingShuffles;
    private boolean leader;
    private Map<Integer, Long> shuffleTimes = new HashMap<Integer, Long>();
    private ArrayList<TimeoutId> partitionRequestList;
    int latestRttRingBufferPointer = 0;
    private long[] latestRtts;
    // This is a routing table maintaining a a list of descriptors for each category and its partitions.
    private Map<MsConfig.Categories, Map<Integer, HashSet<VodDescriptor>>> routingTable;

    private LinkedList<PartitionHelper.PartitionInfo> partitionHistory;
    private static final int HISTORY_LENGTH = 5;

    Comparator<VodDescriptor> peerConnectivityComparator = new Comparator<VodDescriptor>() {
        @Override
        public int compare(VodDescriptor t0, VodDescriptor t1) {
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

        private int compareAvgRtt(VodDescriptor t0, VodDescriptor t1) {
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
        subscribe(handleIndexHashExchangeRequest, gradientRoutingPort);
        subscribe(handleReplicationPrepareCommit, gradientRoutingPort);
        subscribe(handleSearchRequest, gradientRoutingPort);
        subscribe(handleReplicationCommit, gradientRoutingPort);
        subscribe(handleLeaderLookupRequestTimeout, timerPort);
        subscribe(handleSearchResponse, networkPort);
        subscribe(handleSearchRequestTimeout, timerPort);
        subscribe(handleViewSizeRequest, gradientRoutingPort);
//        subscribe(checkPartitioningRequirementHandler, gradientRoutingPort);
        subscribe(handlePartitioningUpdate, gradientRoutingPort);
//        subscribe(delayedPartitioningMessageHandler, networkPort);

        subscribe(handleLeaderGroupInformationRequest, gradientRoutingPort);
        subscribe(handleFailureDetector, fdPort);
		subscribe(handlerControlMessageExchangeInitiation, gradientRoutingPort);
        subscribe(handlerCheckPartitioningUpdate, gradientRoutingPort);
        subscribe(handlerCheckPartitioningInfoRequest, gradientRoutingPort);

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
            routingTable = new HashMap<MsConfig.Categories, Map<Integer, HashSet<VodDescriptor>>>();
            leader = false;
            latestRtts = new long[config.getLatestRttStoreLimit()];
            partitionRequestList = new ArrayList<TimeoutId>();
            partitionHistory = new LinkedList<>();      // Store the history of partitions but upto a specified level.

            SchedulePeriodicTimeout rst = new SchedulePeriodicTimeout(config.getShufflePeriod(), config.getShufflePeriod());
            rst.setTimeoutEvent(new GradientRound(rst, self.getId()));
            trigger(rst, timerPort);
        }
    };

    private void removeNodeFromRoutingTable(VodAddress nodeToRemove)
    {
        MsConfig.Categories category = categoryFromCategoryId(nodeToRemove.getCategoryId());
        Map<Integer, HashSet<VodDescriptor>> categoryRoutingMap = routingTable.get(category);
        if(categoryRoutingMap != null) {
            Set<VodDescriptor> bucket = categoryRoutingMap.get(nodeToRemove.getPartitionId());

            if (bucket != null) {
                Iterator<VodDescriptor> i = bucket.iterator();
                while (i.hasNext()) {
                    VodDescriptor descriptor = i.next();

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

            removeNodeFromLocalState(suspectedNode);
        }
    }
    private void removeNodeFromLocalState(VodAddress nodeAddress)
    {
        //remove suspected node from gradient view
        gradientView.remove(nodeAddress);

        //remove suspected node from routing table
        removeNodeFromRoutingTable(nodeAddress);

        //remove suspected nodes from rtt store
        RTTStore.removeSamples(nodeAddress.getId(), nodeAddress);
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
    private void initiateShuffle(VodDescriptor exchangePartner) {
        Set<VodDescriptor> exchangeNodes = gradientView.getExchangeDescriptors(exchangePartner, config.getShuffleLength());

        ScheduleTimeout rst = new ScheduleTimeout(config.getShufflePeriod());
        rst.setTimeoutEvent(new GradientShuffleMessage.RequestTimeout(rst, self.getId()));
        UUID rTimeoutId = (UUID) rst.getTimeoutEvent().getTimeoutId();
        outstandingShuffles.put(rTimeoutId, exchangePartner.getVodAddress());

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

            Collection<VodDescriptor> sample = event.getVodDescriptors();

            VodAddress selfVodAddress = self.getAddress();
            boolean isNeverBefore = selfVodAddress.getPartitioningType() == VodAddress.PartitioningType.NEVER_BEFORE;

            Set<VodDescriptor> updatedSample = new HashSet<VodDescriptor>();
            if (!isNeverBefore) {

                int bitsToCheck = selfVodAddress.getPartitionIdDepth();
                boolean isOnceBefore = selfVodAddress.getPartitioningType() == VodAddress.PartitioningType.ONCE_BEFORE;
                for (VodDescriptor d : sample) {
                    PartitionId partitionId = PartitionHelper.determineVodDescriptorPartition(d,
                            isOnceBefore, bitsToCheck);
                    VodAddress a = PartitionHelper.updatePartitionId(d.getVodAddress(), partitionId);
                    updatedSample.add(new VodDescriptor(a, d.getUtility(),
                            d.getAge(), d.getMtu(), d.getNumberOfIndexEntries()));
                }
            } else {
                for (VodDescriptor d : sample) {
                    VodAddress a = PartitionHelper.updatePartitionId(d.getVodAddress(),
                            new PartitionId(VodAddress.PartitioningType.NEVER_BEFORE, 1, 0));
                    updatedSample.add(new VodDescriptor(a, d.getUtility(),
                            d.getAge(), d.getMtu(), d.getNumberOfIndexEntries()));
                }
            }

            // Remove all samples from other partitions
//            Iterator<VodDescriptor> iterator = descriptors.iterator();
            Iterator<VodDescriptor> iterator = updatedSample.iterator();
            Set<VodDescriptor> toRemove = new HashSet<VodDescriptor>();
            while (iterator.hasNext()) {
                VodDescriptor d = iterator.next();
                VodAddress next = d.getVodAddress();
                if (next.getPartitionId() != selfVodAddress.getPartitionId()
                        || next.getPartitionIdDepth() != selfVodAddress.getPartitionIdDepth()
                        || next.getPartitioningType() != selfVodAddress.getPartitioningType()) {
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
            SortedSet<VodDescriptor> view = new TreeSet<VodDescriptor>(gradientView.getAll());

            VodAddress selfVodAddress = self.getAddress();
            Iterator<VodDescriptor> iterator = view.iterator();
            while (iterator.hasNext()) {
                VodAddress next = iterator.next().getVodAddress();
                if (next.getPartitionId() != selfVodAddress.getPartitionId()
                        || next.getPartitionIdDepth() != selfVodAddress.getPartitionIdDepth()
                        || next.getPartitioningType() != selfVodAddress.getPartitioningType()) {
                    iterator.remove();
                }
            }

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
            List<VodDescriptor> sample = event.getNodes();
            List<VodDescriptor> updatedSample = new ArrayList<VodDescriptor>();

            VodAddress selfVodAddress = self.getAddress();

            if ((self.getAddress().getPartitioningType() != VodAddress.PartitioningType.NEVER_BEFORE)) {
                boolean isOnePartition = self.getAddress().getPartitioningType() == VodAddress.PartitioningType.ONCE_BEFORE;
                if (!isOnePartition) {
                    int bitsToCheck = self.getAddress().getPartitionIdDepth();

                    for (VodDescriptor d : sample) {
                        PartitionId partitionId = PartitionHelper.determineVodDescriptorPartition(d,
                                isOnePartition, bitsToCheck);

                        VodAddress a = PartitionHelper.updatePartitionId(d.getVodAddress(), partitionId);
                        updatedSample.add(new VodDescriptor(a, d.getUtility(),
                                d.getAge(), d.getMtu(), d.getNumberOfIndexEntries()));

                    }
                } else {
                    for (VodDescriptor d : sample) {
                        PartitionId partitionId = PartitionHelper.determineVodDescriptorPartition(d,
                                isOnePartition, 1);

                        VodAddress a = PartitionHelper.updatePartitionId(d.getVodAddress(), partitionId);
                        updatedSample.add(new VodDescriptor(a, d.getUtility(),
                                d.getAge(), d.getMtu(), d.getNumberOfIndexEntries()));

                    }
                }
            }

            incrementRoutingTableAge();
//            addRoutingTableEntries(sample);
            if ((self.getAddress().getPartitioningType() != VodAddress.PartitioningType.NEVER_BEFORE))
                addRoutingTableEntries(updatedSample);
            else {
                updatedSample = sample;
                addRoutingTableEntries(updatedSample);
            }

            // Remove all samples from other partitions
//            Iterator<VodDescriptor> iterator = sample.iterator();
            Iterator<VodDescriptor> iterator = updatedSample.iterator();
            while (iterator.hasNext()) {
                VodAddress next = iterator.next().getVodAddress();
                if (next.getCategoryId() != selfVodAddress.getCategoryId()
                        || next.getPartitionId() != selfVodAddress.getPartitionId()
                        || next.getPartitionIdDepth() != selfVodAddress.getPartitionIdDepth()
                        || next.getPartitioningType() != selfVodAddress.getPartitioningType()) {
                    iterator.remove();
                }
            }

            //Merge croupier sample to have quicker convergence of gradient
            gradientView.merge(updatedSample);

            // Shuffle with one sample from our partition
//            if (sample.size() > 0) {
//                int n = random.nextInt(sample.size());
//                initiateShuffle(sample.get(n));
//            }
            if (updatedSample.size() > 0) {
                int n = random.nextInt(updatedSample.size());
                initiateShuffle(updatedSample.get(n));
            }
        }
    };

    private void incrementRoutingTableAge() {
        for (Map<Integer, HashSet<VodDescriptor>> categoryRoutingMap : routingTable.values()) {
            for (HashSet<VodDescriptor> bucket : categoryRoutingMap.values()) {
                for (VodDescriptor descriptor : bucket) {
                    descriptor.incrementAndGetAge();
                }
            }
        }
    }

    private void addRoutingTableEntries(List<VodDescriptor> nodes) {
        for (VodDescriptor vodDescriptor : nodes) {
            MsConfig.Categories category = categoryFromCategoryId(vodDescriptor.getVodAddress().getCategoryId());
            int partition = vodDescriptor.getVodAddress().getPartitionId();

            Map<Integer, HashSet<VodDescriptor>> categoryRoutingMap = routingTable.get(category);
            if (categoryRoutingMap == null) {
                categoryRoutingMap = new HashMap<Integer, HashSet<VodDescriptor>>();
                routingTable.put(category, categoryRoutingMap);
            }

            HashSet<VodDescriptor> bucket = categoryRoutingMap.get(partition);
            if (bucket == null) {
                bucket = new HashSet<VodDescriptor>();
                categoryRoutingMap.put(partition, bucket);

                //update old routing tables if see an entry from a new partition
                PartitionId newPartitionId = new PartitionId(vodDescriptor.getVodAddress().getPartitioningType(),
                        vodDescriptor.getVodAddress().getPartitionIdDepth(), vodDescriptor.getVodAddress().getPartitionId());
                updateBucketsInRoutingTable(newPartitionId, categoryRoutingMap, bucket);
            }

            bucket.add(vodDescriptor);
            // keep the best descriptors in this partition
            TreeSet<VodDescriptor> sortedBucket = sortByConnectivity(bucket);
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
    final private HashSet<VodDescriptor> queriedNodes = new HashSet<VodDescriptor>();
    final private HashMap<TimeoutId, VodDescriptor> openRequests = new HashMap<TimeoutId, VodDescriptor>();
    final private HashMap<VodAddress, Integer> locatedLeaders = new HashMap<VodAddress, Integer>();
    private List<VodAddress> leadersAlreadyComunicated = new ArrayList<VodAddress>();

    final Handler<GradientRoutingPort.AddIndexEntryRequest> handleAddIndexEntryRequest = new Handler<GradientRoutingPort.AddIndexEntryRequest>() {
        @Override
        public void handle(GradientRoutingPort.AddIndexEntryRequest event) {
            MsConfig.Categories selfCategory = categoryFromCategoryId(self.getAddress().getCategoryId());
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
                    return;
                }

                NavigableSet<VodDescriptor> startNodes = new TreeSet<VodDescriptor>(utilityComparator);
                startNodes.addAll(gradientView.getAll());

                //Also add nodes from croupier sample to have more chances of getting higher utility nodes, this works
                //as a finger table to random nodes
                Map<Integer, HashSet<VodDescriptor>> croupierPartitions = routingTable.get(selfCategory);
                if (croupierPartitions != null && !croupierPartitions.isEmpty()) {
                    HashSet<VodDescriptor> croupierNodes =  croupierPartitions.get(self.getAddress().getPartitionId());
                    if(croupierNodes != null && !croupierNodes.isEmpty()) {
                        startNodes.addAll(croupierNodes);
                    }
                }

                // Higher utility nodes are further away in the sorted set
                Iterator<VodDescriptor> iterator = startNodes.descendingIterator();

                for (int i = 0; i < LeaderLookupMessage.QueryLimit && iterator.hasNext(); i++) {
                    VodDescriptor node = iterator.next();
                    sendLeaderLookupRequest(node);
                }
            } else {
                Map<Integer, HashSet<VodDescriptor>> partitions = routingTable.get(addCategory);
                if (partitions == null || partitions.isEmpty()) {
                    logger.info("{} handleAddIndexEntryRequest: no partition for category {} ", self.getAddress(), addCategory);
                    return;
                }

                ArrayList<Integer> categoryPartitionsIds = new ArrayList<Integer>(partitions.keySet());
                int categoryPartitionId = (int) (Math.random() * categoryPartitionsIds.size());

                HashSet<VodDescriptor> startNodes = partitions.get(categoryPartitionsIds.get(categoryPartitionId));
                if (startNodes == null) {
                    logger.info("{} handleAddIndexEntryRequest: no nodes for partition {} ", self.getAddress(),
                            categoryPartitionsIds.get(categoryPartitionId));
                    return;
                }

                // Need to sort it every time because values like RTT might have been changed
                SortedSet<VodDescriptor> sortedStartNodes = sortByConnectivity(startNodes);
                Iterator iterator = sortedStartNodes.iterator();

                for (int i = 0; i < LeaderLookupMessage.QueryLimit && iterator.hasNext(); i++) {
                    VodDescriptor node = (VodDescriptor) iterator.next();
                    sendLeaderLookupRequest(node);
                }
            }
        }
    };
    final Handler<LeaderLookupMessage.RequestTimeout> handleLeaderLookupRequestTimeout = new Handler<LeaderLookupMessage.RequestTimeout>() {
        @Override
        public void handle(LeaderLookupMessage.RequestTimeout event) {
            VodDescriptor unresponsiveNode = openRequests.remove(event.getTimeoutId());
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
            TreeSet<VodDescriptor> higherNodes = new TreeSet<VodDescriptor>(gradientView.getHigherUtilityNodes());
            ArrayList<VodDescriptor> vodDescriptors = new ArrayList<VodDescriptor>();

            // Higher utility nodes are further away in the sorted set
            Iterator<VodDescriptor> iterator = higherNodes.descendingIterator();
            while (vodDescriptors.size() < LeaderLookupMessage.ResponseLimit && iterator.hasNext()) {
                vodDescriptors.add(iterator.next());
            }

            // Some space left, also return lower nodes
            if (vodDescriptors.size() < LeaderLookupMessage.ResponseLimit) {
                TreeSet<VodDescriptor> lowerNodes = new TreeSet<VodDescriptor>(gradientView.getLowerUtilityNodes());
                iterator = lowerNodes.iterator();
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
                List<VodDescriptor> higherUtilityNodes = event.getVodDescriptors();

                if (higherUtilityNodes.size() > LeaderLookupMessage.QueryLimit) {
                    Collections.sort(higherUtilityNodes, utilityComparator);
                    // Higher utility nodes are further away
                    Collections.reverse(higherUtilityNodes);
                }

                // If the lowest returned nodes is an announced leader, increment it's counter
                if (higherUtilityNodes.size() > 0) {
                    VodDescriptor first = higherUtilityNodes.get(0);
                    if (locatedLeaders.containsKey(first.getVodAddress())) {
                        Integer numberOfAnswers = locatedLeaders.get(first.getVodAddress()) + 1;
                        locatedLeaders.put(first.getVodAddress(), numberOfAnswers);
                    }
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

                    if(!leadersAlreadyComunicated.contains(locatedLeader)){
                        trigger(new AddIndexEntryMessage.Request(self.getAddress(), locatedLeader, addIndexEntryRequestTimeoutId, indexEntryToAdd), networkPort);
                        leadersAlreadyComunicated.add(locatedLeader);
                    }
                }
            }
        }
    };

    private void sendLeaderLookupRequest(VodDescriptor node) {
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
    final Handler<GradientRoutingPort.IndexHashExchangeRequest> handleIndexHashExchangeRequest = new Handler<GradientRoutingPort.IndexHashExchangeRequest>() {
        @Override
        public void handle(GradientRoutingPort.IndexHashExchangeRequest event) {
            ArrayList<VodDescriptor> nodes = new ArrayList<VodDescriptor>(gradientView.getHigherUtilityNodes());
            if (nodes.isEmpty() || nodes.size() < event.getNumberOfRequests()) {
                logger.warn(" {}: Not enough nodes to perform Index Hash Exchange." + self.getAddress().getId());
                return;
            }

            for (int i = 0; i < event.getNumberOfRequests(); i++) {
                int n = random.nextInt(nodes.size());
                VodDescriptor node = nodes.get(n);
                nodes.remove(node);

                trigger(new IndexHashExchangeMessage.Request(self.getAddress(), node.getVodAddress(), event.getTimeoutId(),
                        event.getLowestMissingIndexEntry(), event.getExistingEntries()), networkPort);
            }
        }
    };
    final Handler<GradientRoutingPort.SearchRequest> handleSearchRequest = new Handler<GradientRoutingPort.SearchRequest>() {
        @Override
        public void handle(GradientRoutingPort.SearchRequest event) {
            MsConfig.Categories category = event.getPattern().getCategory();
            Map<Integer, HashSet<VodDescriptor>> categoryRoutingMap = routingTable.get(category);

            if (categoryRoutingMap == null) {
                return;
            }

            trigger(new NumberOfPartitions(event.getTimeoutId(), categoryRoutingMap.keySet().size()), gradientRoutingPort);

            for (Integer partition : categoryRoutingMap.keySet()) {
                // if your partition, hit only self
                if (partition == self.getAddress().getPartitionId()
                        && category == categoryFromCategoryId(self.getAddress().getCategoryId())) {
                    trigger(new SearchMessage.Request(self.getAddress(), self.getAddress(),
                            event.getTimeoutId(), event.getTimeoutId(), event.getPattern(),
                            partition), networkPort);

                    continue;
                }

                TreeSet<VodDescriptor> bucket = sortByConnectivity(categoryRoutingMap.get(partition));
                TreeSet<VodDescriptor> unconnectedNodes = null;
                Iterator<VodDescriptor> iterator = bucket.iterator();
                for (int i = 0; i < config.getSearchParallelism() && iterator.hasNext(); i++) {
                    VodDescriptor vodDescriptor = iterator.next();

                    RTTStore.RTT rtt = RTTStore.getRtt(vodDescriptor.getId(), vodDescriptor.getVodAddress());
                    double latestRttsAvg = getLatestRttsAvg();
                    if (rtt != null && latestRttsAvg != 0 && rtt.getRttStats().getAvgRTT() > (config.getRttAnomalyTolerance() * latestRttsAvg)) {
                        if (unconnectedNodes == null) {
                            unconnectedNodes = getUnconnectedNodes(bucket);
                        }

                        if (!unconnectedNodes.isEmpty()) {
                            vodDescriptor = unconnectedNodes.pollFirst();
                        }
                    }

                    ScheduleTimeout scheduleTimeout = new ScheduleTimeout(event.getQueryTimeout());
                    scheduleTimeout.setTimeoutEvent(new SearchMessage.RequestTimeout(scheduleTimeout, self.getId(), vodDescriptor));
                    trigger(scheduleTimeout, timerPort);
                    trigger(new SearchMessage.Request(self.getAddress(), vodDescriptor.getVodAddress(),
                            scheduleTimeout.getTimeoutEvent().getTimeoutId(), event.getTimeoutId(), event.getPattern(),
                            partition), networkPort);

                    shuffleTimes.put(scheduleTimeout.getTimeoutEvent().getTimeoutId().getId(), System.currentTimeMillis());
                    vodDescriptor.setConnected(true);
                }
            }
        }
    };
    final Handler<SearchMessage.Response> handleSearchResponse = new Handler<SearchMessage.Response>() {
        @Override
        public void handle(SearchMessage.Response event) {
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
            VodDescriptor unresponsiveNode = event.getVodDescriptor();

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
            PublicKey key = publicKeyBroadcast.getPublicKey();

            for (VodDescriptor item : gradientView.getAll()) {
                trigger(new PublicKeyMessage(self.getAddress(), item.getVodAddress().getNodeAddress(), key), networkPort);
            }
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

            for (VodDescriptor item : gradientView.getLowerUtilityNodes()) {
                trigger(new PublicKeyMessage(publicKeyMessage.getVodSource(), item.getVodAddress().getNodeAddress(), key), networkPort);
            }
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
            NavigableSet<VodDescriptor> lowerUtilityNodes = ((NavigableSet)gradientView.getLowerUtilityNodes()).descendingSet();
            List<VodAddress> leaderGroupAddresses = new ArrayList<>();

            // If gradient not full or not enough nodes in leader group.
            if((gradientView.getAll().size() < config.getViewSize())|| (lowerUtilityNodes.size() < leaderGroupSize)){
                trigger(new LeaderGroupInformation.Response(event.getMedianId(),event.getPartitioningType(), leaderGroupAddresses), gradientRoutingPort);
                return;
            }

            int i=0;
            for(VodDescriptor desc : lowerUtilityNodes){

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

        PartitionId selfPartitionId = new PartitionId(partitionsNumber, self.getAddress().getPartitionIdDepth(),
                self.getAddress().getPartitionId());

        boolean partitionSubId = PartitionHelper.determineYourNewPartitionSubId(nodeId, selfPartitionId);

        if (partitionsNumber == VodAddress.PartitioningType.NEVER_BEFORE) {
            int partitionId = (partitionSubId ? 1 : 0);

            int selfCategory = self.getAddress().getCategoryId();
            int newOverlayId = PartitionHelper.encodePartitionDataAndCategoryIdAsInt(VodAddress.PartitioningType.ONCE_BEFORE,
                    1, partitionId, selfCategory);

            // TODO - all existing VodAddresses in Sets, Maps, etc are now invalid.
            // Do we replace them or what do we do with them?
            ((MsSelfImpl) self).setOverlayId(newOverlayId);

        } else {
            int newPartitionId = self.getAddress().getPartitionId() | ((partitionSubId ? 1 : 0) << self.getAddress().getPartitionIdDepth());
            int selfCategory = self.getAddress().getCategoryId();

            // Incrementing partitioning depth in the overlayId.
            int newOverlayId = PartitionHelper.encodePartitionDataAndCategoryIdAsInt(VodAddress.PartitioningType.MANY_BEFORE,
                    self.getAddress().getPartitionIdDepth()+1, newPartitionId, selfCategory);
            ((MsSelfImpl) self).setOverlayId(newOverlayId);
        }
        logger.debug("Partitioning Occured at Node: " + self.getId() + " PartitionDepth: " + self.getAddress().getPartitionIdDepth() +" PartitionId: " + self.getAddress().getPartitionId() + " PartitionType: " + self.getAddress().getPartitioningType());
        int partitionId = self.getAddress().getPartitionId();
        Snapshot.updateInfo(self.getAddress());                 // Overlay id present in the snapshot not getting updated, so added the method.
        Snapshot.addPartition(new Pair<Integer, Integer>(self.getAddress().getCategoryId(), partitionId));
        return partitionSubId;
    }



    private MsConfig.Categories categoryFromCategoryId(int categoryId) {
        return MsConfig.Categories.values()[categoryId];
    }

    private TreeSet<VodDescriptor> sortByConnectivity(Collection<VodDescriptor> vodDescriptors) {
        // Need to sort it every time because values like MsSelfImpl.RTT might have been changed
        TreeSet<VodDescriptor> sortedVodDescriptors = new TreeSet<VodDescriptor>(vodDescriptors);
        return sortedVodDescriptors;
    }

    private TreeSet<VodDescriptor> getUnconnectedNodes(Collection<VodDescriptor> vodDescriptors) {
        TreeSet<VodDescriptor> unconnectedNodes = new TreeSet<VodDescriptor>(peerConnectivityComparator);
        for (VodDescriptor vodDescriptor : vodDescriptors) {
            if (vodDescriptor.isConnected() == false) {
                unconnectedNodes.add(vodDescriptor);
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
            trigger(new LeaderStatusPort.TerminateBeingLeader(), leaderStatusPort);
            // Leader boolean simply sends true down the message in case of leader node, as it was implemented like this way before, not sure why.
            boolean partition = determineYourPartitionAndUpdatePartitionsNumberUpdated(update.getPartitioningTypeInfo());
            gradientView.adjustViewToNewPartitions();

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

            ArrayList<VodDescriptor> higherUtilityNodes = new ArrayList<VodDescriptor>(gradientView.getHigherUtilityNodes());

            // TODO: update the check to allow to send to nearby neighbors the request.
            if(higherUtilityNodes == null || higherUtilityNodes.size() < event.getControlMessageExchangeNumber())
                return;

            // FIXME: Correct the issue of repeating of numbers.
            // Provide the list of requested nodes.
            for(int i =0 ; i< event.getControlMessageExchangeNumber() ; i++){
                int n = random.nextInt(higherUtilityNodes.size());
                VodAddress destination = higherUtilityNodes.get(n).getVodAddress();
                trigger(new ControlMessage.Request(self.getAddress(),destination,event.getRoundId()), networkPort);
            }
        }
    };


    /**
     * Request To check if the source address is behind in terms of partitioning updates.
     */
    Handler<CheckPartitionInfoHashUpdate.Request> handlerCheckPartitioningUpdate = new Handler<CheckPartitionInfoHashUpdate.Request>(){
        @Override
        public void handle(CheckPartitionInfoHashUpdate.Request event) {

            // Check for the partitioning updates and return it back.
            logger.debug("Check Partitioning Update Received.");

            LinkedList<PartitionHelper.PartitionInfoHash> partitionUpdateHashes = new LinkedList<>();
            ControlMessageEnum controlMessageEnum;

            // Check for the responses when you have atleast partitioned yourself.
            if(self.getAddress().getPartitioningType() != VodAddress.PartitioningType.NEVER_BEFORE){

                controlMessageEnum = fetchPartitioningHashUpdatesMessageEnum(event.getSourceAddress(), partitionUpdateHashes);
            }
            else{
                // Send empty partition update as you have not partitioned.
                controlMessageEnum = ControlMessageEnum.PARTITION_UPDATE;
            }

            // Trigger the partitioning update.
            trigger(new CheckPartitionInfoHashUpdate.Response(event.getRoundId(),event.getSourceAddress(), partitionUpdateHashes, controlMessageEnum), gradientRoutingPort);
        }
    };


    /**
     * Based on the source address, provide the control message enum that needs to be associated with the control response object.
     */
    private ControlMessageEnum fetchPartitioningHashUpdatesMessageEnum(VodAddress address , List<PartitionHelper.PartitionInfoHash> partitionUpdateHashes){

        boolean isOnePartition = self.getAddress().getPartitioningType() == VodAddress.PartitioningType.ONCE_BEFORE;

        // for ONE_BEFORE
        if(isOnePartition){
            if(address.getPartitioningType() == VodAddress.PartitioningType.NEVER_BEFORE){
                for(PartitionHelper.PartitionInfo partitionInfo: partitionHistory)
                    partitionUpdateHashes.add(new PartitionHelper.PartitionInfoHash(partitionInfo));
            }
        }

        // for MANY_BEFORE.
        else {

            int myDepth = self.getAddress().getPartitionIdDepth();
            if (address.getPartitioningType() == VodAddress.PartitioningType.NEVER_BEFORE) {

                if (myDepth <= (HISTORY_LENGTH)) {
                    for(PartitionHelper.PartitionInfo partitionInfo: partitionHistory)
                        partitionUpdateHashes.add(new PartitionHelper.PartitionInfoHash(partitionInfo));
                }
                else
                    return ControlMessageEnum.REJOIN;
            }
            else {

                int receivedNodeDepth = address.getPartitionIdDepth();
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

        LinkedList<PartitionHelper.PartitionInfo> partitionUpdates = new LinkedList<>();

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
