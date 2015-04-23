package se.sics.ms.search;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.commons.codec.binary.Base64;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.*;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.co.FailureDetectorPort;
import se.sics.gvod.common.msgs.MessageDecodingException;
import se.sics.gvod.common.msgs.MessageEncodingException;
import se.sics.gvod.config.SearchConfiguration;
import se.sics.gvod.net.VodAddress;
import se.sics.kompics.*;
import se.sics.kompics.network.Network;
import se.sics.kompics.network.Transport;
import se.sics.kompics.timer.*;
import se.sics.kompics.timer.Timer;
import se.sics.ms.aggregator.SearchComponentUpdate;
import se.sics.ms.aggregator.SearchComponentUpdateEvent;
import se.sics.ms.aggregator.port.StatusAggregatorPort;
import se.sics.ms.common.*;
import se.sics.ms.configuration.MsConfig;
import se.sics.ms.control.*;
import se.sics.ms.data.*;
import se.sics.ms.election.aggregation.ElectionLeaderComponentUpdate;
import se.sics.ms.election.aggregation.ElectionLeaderUpdateEvent;
import se.sics.ms.events.UiAddIndexEntryRequest;
import se.sics.ms.events.UiAddIndexEntryResponse;
import se.sics.ms.events.UiSearchRequest;
import se.sics.ms.events.UiSearchResponse;
import se.sics.ms.gradient.control.*;
import se.sics.ms.gradient.events.*;
import se.sics.ms.gradient.ports.GradientRoutingPort;
import se.sics.ms.gradient.ports.LeaderStatusPort;
import se.sics.ms.messages.*;
import se.sics.ms.model.LocalSearchRequest;
import se.sics.ms.model.PeerControlRequestInfoHolder;
import se.sics.ms.model.ReplicationCount;
import se.sics.ms.ports.SelfChangedPort;
import se.sics.ms.ports.SimulationEventsPort;
import se.sics.ms.ports.SimulationEventsPort.AddIndexSimulated;
import se.sics.ms.ports.UiPort;
import se.sics.ms.timeout.AwaitingForCommitTimeout;
import se.sics.ms.timeout.PartitionCommitTimeout;
import se.sics.ms.types.*;
import se.sics.ms.types.OverlayId;
import se.sics.ms.util.*;
import se.sics.p2ptoolbox.croupier.CroupierPort;
import se.sics.p2ptoolbox.croupier.msg.CroupierUpdate;
import se.sics.p2ptoolbox.election.api.msg.ElectionState;
import se.sics.p2ptoolbox.election.api.msg.LeaderState;
import se.sics.p2ptoolbox.election.api.msg.LeaderUpdate;
import se.sics.p2ptoolbox.election.api.msg.ViewUpdate;
import se.sics.p2ptoolbox.election.api.ports.LeaderElectionPort;
import se.sics.p2ptoolbox.gradient.GradientPort;
import se.sics.p2ptoolbox.gradient.msg.GradientUpdate;
import se.sics.p2ptoolbox.util.network.impl.BasicAddress;
import se.sics.p2ptoolbox.util.network.impl.BasicContentMsg;
import se.sics.p2ptoolbox.util.network.impl.DecoratedAddress;
import se.sics.p2ptoolbox.util.network.impl.DecoratedHeader;
import sun.misc.BASE64Encoder;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.security.*;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.util.*;
import java.util.UUID;
import java.util.logging.Level;


/**
 * This class handles the storing, adding and searching for indexes. It acts in
 * two different modes depending on if it the executing node was elected leader
 * or not.
 * <p/>
 * {@link IndexEntry}s are spread via gossiping using the Cyclon samples stored
 * in the routing tables for the partition of the local node.
 */
public final class Search extends ComponentDefinition {
    /**
     * Set to true to store the Lucene index on disk
     */
    public static final boolean PERSISTENT_INDEX = false;

    // ====== PORTS.
    Positive<SimulationEventsPort> simulationEventsPort = positive(SimulationEventsPort.class);
    Positive<Network> networkPort = positive(Network.class);
    Positive<Timer> timerPort = positive(Timer.class);
    Positive<GradientRoutingPort> gradientRoutingPort = positive(GradientRoutingPort.class);
    Positive<FailureDetectorPort> fdPort = requires(FailureDetectorPort.class);
    Negative<LeaderStatusPort> leaderStatusPort = negative(LeaderStatusPort.class);
    Negative<UiPort> uiPort = negative(UiPort.class);
    Negative<SelfChangedPort> selfChangedPort = negative(SelfChangedPort.class);
    Positive<CroupierPort> croupierPortPositive = requires(CroupierPort.class);
    Positive<StatusAggregatorPort> statusAggregatorPortPositive = requires(StatusAggregatorPort.class);
    Positive<GradientPort> gradientPort = requires(GradientPort.class);
    Positive<LeaderElectionPort> electionPort = requires(LeaderElectionPort.class);

    // ======== LOCAL VARIABLES.
    private static final Logger logger = LoggerFactory.getLogger(Search.class);
    private ApplicationSelf self;
    private SearchConfiguration config;
    private boolean leader;
    private long lowestMissingIndexValue;
    private SortedSet<Long> existingEntries;
    private long nextInsertionId;

    private Map<UUID, ReplicationCount> replicationRequests;
    private Map<UUID, ReplicationCount> commitRequests;
    private Map<Long, UUID> gapTimeouts;
    private Map<java.util.UUID, Long> recentRequests;

    // Apache Lucene used for searching
    private StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_42);
    private Directory index;
    private IndexWriterConfig indexWriterConfig = new IndexWriterConfig(Version.LUCENE_42, analyzer);

    // Lucene variables used to store and search in collected answers
    private LocalSearchRequest searchRequest;
    private Directory searchIndex;


    // Leader Election Protocol.
    private java.util.UUID electionRound = java.util.UUID.randomUUID();

    // Aggregator Variable.
    private int defaultComponentOverlayId = 0;

    private PrivateKey privateKey;
    private PublicKey publicKey;
    private ArrayList<PublicKey> leaderIds = new ArrayList<PublicKey>();
    private HashMap<IndexEntry, java.util.UUID> pendingForCommit = new HashMap<IndexEntry, java.util.UUID>();
    private HashMap<UUID, UUID> replicationTimeoutToAdd = new HashMap<UUID, UUID>();
    private HashMap<UUID, Integer> searchPartitionsNumber = new HashMap<UUID, Integer>();

    private HashMap<PartitionHelper.PartitionInfo, UUID> partitionUpdatePendingCommit = new HashMap<PartitionHelper.PartitionInfo, UUID>();
    private long minStoredId = Long.MIN_VALUE;
    private long maxStoredId = Long.MIN_VALUE;

    private HashMap<UUID, Long> timeStoringMap = new HashMap<UUID, Long>();
    private static HashMap<UUID, Pair<Long, Integer>> searchRequestStarted = new HashMap<UUID, Pair<Long, Integer>>();


    // Partitioning Protocol Information.
    private UUID partitionPreparePhaseTimeoutId;
    private UUID partitionCommitPhaseTimeoutId;

    private UUID partitionRequestId;
    private boolean partitionInProgress = false;

    // Generic Control Pull Mechanism.
    private java.util.UUID controlMessageExchangeRoundId;
    private Map<BasicAddress, UUID> peerControlRequestAddressIdMap = new HashMap<BasicAddress, UUID>();      // FIXME: Needs to be refactored in one message.
    private Map<BasicAddress, PeerControlRequestInfoHolder> peerControlResponseMap = new HashMap<BasicAddress, PeerControlRequestInfoHolder>();
    private Map<ControlMessageResponseTypeEnum, List<? extends ControlBase>> controlMessageResponseHolderMap = new HashMap<ControlMessageResponseTypeEnum, List<? extends ControlBase>>();

    private int controlMessageResponseCount = 0;
    private boolean partitionUpdateFetchInProgress = false;
    private UUID currentPartitionInfoFetchRound;

    private LinkedList<PartitionHelper.PartitionInfo> partitionHistory;
    private static final int HISTORY_LENGTH = 5;
    private LuceneAdaptor writeLuceneAdaptor;
    private LuceneAdaptor searchRequestLuceneAdaptor;

    // Leader Election Protocol.
    private Collection<DecoratedAddress> leaderGroupInformation;
    // Trackers.
    private MultipleEntryAdditionTracker entryAdditionTracker;
    private Map<UUID, UUID> entryPrepareTimeoutMap; // (roundId, prepareTimeoutId).
    private PartitioningTracker partitioningTracker;

    /**
     * Timeout for waiting for an {@link se.sics.ms.messages.AddIndexEntryMessage.Response} acknowledgment for an
     * {@link se.sics.ms.messages.AddIndexEntryMessage.Response} request.
     */
    private static class AddIndexTimeout extends Timeout {
        private final int retryLimit;
        private int numberOfRetries = 0;
        private final IndexEntry entry;

        /**
         * @param request    the ScheduleTimeout that holds the Timeout
         * @param retryLimit the number of retries for the related
         *                   {@link se.sics.ms.messages.AddIndexEntryMessage.Request}
         * @param entry      the {@link se.sics.ms.types.IndexEntry} this timeout was scheduled for
         */
        public AddIndexTimeout(ScheduleTimeout request, int retryLimit, IndexEntry entry) {
            super(request);
            this.retryLimit = retryLimit;
            this.entry = entry;
        }

        /**
         * Increment the number of retries executed.
         */
        public void incrementTries() {
            numberOfRetries++;
        }

        /**
         * @return true if the number of retries exceeded the limit
         */
        public boolean reachedRetryLimit() {
            return numberOfRetries == retryLimit;
        }

        /**
         * @return the {@link IndexEntry} this timeout was scheduled for
         */
        public IndexEntry getEntry() {
            return entry;
        }
    }

    public Search(SearchInit init) {

        doInit(init);
        subscribe(handleStart, control);
        subscribe(handleRound, timerPort);
        subscribe(handleAddIndexSimulated, simulationEventsPort);
        subscribe(handleIndexHashExchangeRequest, networkPort);

        subscribe(handleIndexHashExchangeResponse, networkPort);
//        subscribe(handleIndexHashExchangeResponse, chunkManagerPort);
        subscribe(handleIndexExchangeRequest, networkPort);
        subscribe(handleIndexExchangeResponse, networkPort);
//        subscribe(handleIndexExchangeResponse, chunkManagerPort);

        subscribe(handleAddIndexEntryRequest, networkPort);
        subscribe(preparePhaseTimeout, timerPort);

        subscribe(handleAddIndexEntryResponse, networkPort);
        subscribe(handleSearchRequest, networkPort);
//        subscribe(handleSearchResponse, chunkManagerPort);
        subscribe(handleSearchResponse, networkPort);
        subscribe(handleSearchTimeout, timerPort);

        subscribe(handleAddRequestTimeout, timerPort);
        subscribe(handleRecentRequestsGcTimeout, timerPort);
        subscribe(searchRequestHandler, uiPort);

        subscribe(handleRepairRequest, networkPort);
        subscribe(handleRepairResponse, networkPort);
        subscribe(handlePrepareCommit, networkPort);
        subscribe(handleAwaitingForCommitTimeout, timerPort);

        subscribe(handleEntryAdditionPrepareCommitResponse, networkPort);
        subscribe(handleEntryCommitRequest, networkPort);
        subscribe(addIndexEntryRequestHandler, uiPort);

        subscribe(handleSearchSimulated, simulationEventsPort);
        subscribe(handleIndexExchangeTimeout, timerPort);
        subscribe(handleNumberOfPartitions, gradientRoutingPort);

        // Two Phase Commit Mechanism.
        subscribe(partitionPrepareTimeoutHandler, timerPort);
        subscribe(handlerPartitionPrepareRequest, networkPort);
        subscribe(handlerPartitionPrepareResponse, networkPort);
        subscribe(handlePartitionCommitTimeout, timerPort);

        subscribe(handlerPartitionCommitRequest, networkPort);
        subscribe(handlerPartitionCommitResponse, networkPort);
        subscribe(handlerPartitionCommitTimeoutMessage, timerPort);

        // Generic Control message exchange mechanism
        subscribe(handlerControlMessageExchangeRound, timerPort);
        subscribe(handlerControlMessageRequest, networkPort);
        subscribe(handlerControlMessageInternalResponse, gradientRoutingPort);
        subscribe(handlerControlMessageResponse, networkPort);
        subscribe(handlerDelayedPartitioningRequest, networkPort);

        subscribe(delayedPartitioningTimeoutHandler, timerPort);
        subscribe(delayedPartitioningResponseHandler, networkPort);

        // LeaderElection handlers.
        subscribe(leaderElectionHandler, electionPort);
        subscribe(terminateBeingLeaderHandler, electionPort);
        subscribe(leaderUpdateHandler, electionPort);
        subscribe(enableLGMembershipHandler, electionPort);
        subscribe(disableLGMembershipHandler, electionPort);
    }

    /**
     * Initialize the component.
     */
    private void doInit(SearchInit init) {

        self = init.getSelf();
        config = init.getConfiguration();
        publicKey = init.getPublicKey();
        privateKey = init.getPrivateKey();

        replicationRequests = new HashMap<UUID, ReplicationCount>();
        nextInsertionId = 0;
        lowestMissingIndexValue = 0;
        commitRequests = new HashMap<UUID, ReplicationCount>();
        existingEntries = new TreeSet<Long>();

        // Tracker.
        partitioningTracker = new PartitioningTracker();
        entryAdditionTracker = new MultipleEntryAdditionTracker(100); // Can hold upto 100 simultaneous requests.
        entryPrepareTimeoutMap = new HashMap<java.util.UUID, java.util.UUID>();

        gapTimeouts = new HashMap<Long, UUID>();
        partitionHistory = new LinkedList<PartitionHelper.PartitionInfo>();      // Store the history of partitions but upto a specified level.
        File file = null;
        if (PERSISTENT_INDEX) {
            file = new File("resources/index_" + self.getId());
            try {
                index = FSDirectory.open(file);
            } catch (IOException e1) {
                // TODO proper exception handling
                e1.printStackTrace();
                System.exit(-1);
            }

        } else {
            index = new RAMDirectory();
        }

        writeLuceneAdaptor = new LuceneAdaptorImpl(index, indexWriterConfig);
        try {
            writeLuceneAdaptor.initialEmptyWriterCommit();

            if (PERSISTENT_INDEX && file != null && file.exists()) {
                initializeIndexCaches(writeLuceneAdaptor);
            }

            minStoredId = ApplicationLuceneQueries.getMinStoredIdFromLucene(writeLuceneAdaptor);
            maxStoredId = ApplicationLuceneQueries.getMaxStoredIdFromLucene(writeLuceneAdaptor);

        } catch (LuceneAdaptorException e) {
            
            logger.warn("{}: Unable to initialize index from file");
            e.printStackTrace();
            throw new RuntimeException("Unable to initialize index from file", e);
        }
        recentRequests = new HashMap<java.util.UUID, Long>();

        if (minStoredId > maxStoredId) {
            long temp = minStoredId;
            minStoredId = maxStoredId;
            maxStoredId = temp;
        }

    }

    /**
     * Initialize the component.
     */
    final Handler<Start> handleStart = new Handler<Start>() {
        public void handle(Start init) {

            logger.debug("{}: Main component initialized", self.getId());
            
            informListeningComponentsAboutUpdates(self);
            
            SchedulePeriodicTimeout rst = new SchedulePeriodicTimeout(
                    config.getRecentRequestsGcInterval(),
                    config.getRecentRequestsGcInterval());
            rst.setTimeoutEvent(new TimeoutCollection.RecentRequestsGcTimeout(rst));
            trigger(rst, timerPort);

            rst = new SchedulePeriodicTimeout(MsConfig.GRADIENT_SHUFFLE_PERIOD, MsConfig.GRADIENT_SHUFFLE_PERIOD);
            rst.setTimeoutEvent(new TimeoutCollection.ExchangeRound(rst));
            trigger(rst, timerPort);

            rst = new SchedulePeriodicTimeout(MsConfig.CONTROL_MESSAGE_EXCHANGE_PERIOD, MsConfig.CONTROL_MESSAGE_EXCHANGE_PERIOD);
            rst.setTimeoutEvent(new TimeoutCollection.ControlMessageExchangeRound(rst));
            trigger(rst, timerPort);
        }
    };

    /**
     * Initialize the Index Caches, from the indexes stored in files.
     *
     * @param luceneAdaptor LuceneAdaptor for access to lucene instance.
     * @throws LuceneAdaptorException
     */
    public void initializeIndexCaches(LuceneAdaptor luceneAdaptor) throws LuceneAdaptorException {

        boolean continuous = true;
        int readLimit = 20000;

        for (long i = 0; ; i += readLimit) {
            Query query = NumericRangeQuery.newLongRange(IndexEntry.ID, i, i + readLimit, true, false);
            List<IndexEntry> indexEntryList = luceneAdaptor.searchIndexEntriesInLucene(query, new Sort(new SortField(IndexEntry.ID, Type.LONG)), readLimit);

            if (indexEntryList.isEmpty()) {
                logger.info("Empty entry list retrieved from the index.");
                break;
            }

            if (continuous) {
                Long[] ids = new Long[indexEntryList.size()];

                for (int j = 0, x = indexEntryList.size(); j < x; j++) {
                    ids[j] = indexEntryList.get(j).getId();
                }

                if (ids[0] != 0 && lowestMissingIndexValue != ids[0]) {
                    continuous = false;
                    Collections.addAll(existingEntries, ids);

                } else {
                    // Search for gaps between the given ids
                    for (int j = 0; j < ids.length; j++) {
                        lowestMissingIndexValue = ids[j] + 1;
                        // If a gap was found add higher ids to the existing
                        // entries
                        if (j + 1 < ids.length && ids[j] + 1 != ids[j + 1]) {
                            continuous = false;
                            existingEntries.addAll(Arrays.asList(ids).subList(j + 1, ids.length));
                            break;
                        }
                    }
                }
            } else {
                for (int j = 0, x = indexEntryList.size(); j < x; j++) {
                    existingEntries.add(indexEntryList.get(j).getId());
                }
            }
        }
    }

    private boolean exchangeInProgress = false;
    private java.util.UUID indexExchangeTimeout;
    private HashMap<DecoratedAddress, Collection<IndexHash>> collectedHashes = new HashMap<DecoratedAddress, Collection<IndexHash>>();
    private HashSet<IndexHash> intersection;

    /**
     * Fetch the number of entries stored in the lucene.
     *
     * @return number of Index Entries.
     */
    private int queryBasedTotalHit(LuceneAdaptor adaptor, Query query) {

        int numberOfEntries = 0;
        TotalHitCountCollector totalHitCountCollector = null;
        try {
            totalHitCountCollector = new TotalHitCountCollector();
            adaptor.searchDocumentsInLucene(query, totalHitCountCollector);
            numberOfEntries = totalHitCountCollector.getTotalHits();

        } catch (LuceneAdaptorException e) {
            logger.error("{}: Unable to retrieve hit count for the query passed {}", self.getId(), query.toString());
            e.printStackTrace();
        }
        return numberOfEntries;
    }


    private int getTotalStoredEntriesCount(LuceneAdaptor luceneAdaptor) throws LuceneAdaptorException {
        return luceneAdaptor.getSizeOfLuceneInstance();
    }


    /**
     * Initiate the control message exchange in the system.
     */
    Handler<TimeoutCollection.ControlMessageExchangeRound> handlerControlMessageExchangeRound = new Handler<TimeoutCollection.ControlMessageExchangeRound>() {
        @Override
        public void handle(TimeoutCollection.ControlMessageExchangeRound event) {

            logger.debug("Initiated the Periodic Exchange Timeout.");

            //Clear the previous rounds data to avoid clash in the responses.
            cleanControlMessageResponseData();
            controlMessageExchangeRoundId = UUID.randomUUID();
            trigger(new GradientRoutingPort.InitiateControlMessageExchangeRound(controlMessageExchangeRoundId, config.getIndexExchangeRequestNumber()), gradientRoutingPort);
        }
    };


    /**
     * Initial handler of the control message request from the nodes in the system.
     */
    ClassMatchedHandler<ControlInformation.Request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, ControlInformation.Request>> handlerControlMessageRequest = new ClassMatchedHandler<ControlInformation.Request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, ControlInformation.Request>>() {
        @Override
        public void handle(ControlInformation.Request request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, ControlInformation.Request> event) {

            logger.debug("{}: Received control message request from : {}", self.getId(), event.getSource());
            BasicAddress basicAddress = event.getSource().getBase();

            if (peerControlResponseMap.get(basicAddress) != null){
                peerControlResponseMap.get(basicAddress).reset();
            }
                
            else{
                peerControlResponseMap.put(basicAddress, 
                        new PeerControlRequestInfoHolder(config.getControlMessageEnumSize()));
            }

            peerControlRequestAddressIdMap.put(basicAddress, request.getRequestId());
            requestComponentsForInfo(request, event.getSource());
        }
    };

    /**
     * Request the components in the system for the information,
     * that will be collated and sent back to the requesting node.
     *
     * @param request Request Information.
     * @param source  Source
     */
    private void requestComponentsForInfo(ControlInformation.Request request, DecoratedAddress source) {

        handleInternalControlResponse(getPartitionInfoHashUpdate(request, source));
        trigger(new CheckLeaderInfoUpdate.Request(request.getRequestId(), source), gradientRoutingPort);
    }

    /**
     * Handle the control message responses from the different components.
     * Aggregate and compress the information which needs to be sent back to the requsting node.
     *
     * @param event Control Message Response event.
     */
    void handleInternalControlResponse(ControlMessageInternal.Response event) {

        try {

            UUID roundIdReceived = event.getRoundId();
            UUID currentRoundId = peerControlRequestAddressIdMap.get(event.getSourceAddress().getBase());

            // Perform initial checks to avoid old responses.
            if (currentRoundId == null || !currentRoundId.equals(roundIdReceived)) {
                logger.warn("{}: Received response from the internal component for an old round. ", self.getId());
                return;
            }

            // Update the peer control response map to add the new entry.
            PeerControlRequestInfoHolder controlMessageResponse = peerControlResponseMap.get(event.getSourceAddress().getBase());
            if (controlMessageResponse == null) {
                logger.error(" Not able to Locate Response Object for Node: " + event.getSourceAddress().getId());
                return;
            }

            // Append the response to the buffer.
            ByteBuf buf = controlMessageResponse.getBuffer();
            ControlMessageEncoderFactory.encodeControlMessageInternal(buf, event); //FIX THIS.

            if (controlMessageResponse.addAndCheckStatus()) {
                
                // FIXME: Timeout preventing infinite waiting.
                logger.debug(" Ready To Send back Control Message Response to the Requestor. ");
                ControlInformation.Response response = new ControlInformation.Response(currentRoundId, buf.array());
                trigger(CommonHelper.getDecoratedContentMessage(self.getAddress(), event.getSourceAddress(), Transport.UDP, response), networkPort);
                cleanControlRequestMessageData(event.getSourceAddress().getBase());
            }
        } catch (MessageEncodingException e) {
            
            logger.warn("{}: Encoding during control response creation failed.", self.getId());
            e.printStackTrace();
            cleanControlRequestMessageData(event.getSourceAddress().getBase());
            throw new RuntimeException("Encoding during control response creation failed.", e);
        }
    }


    /**
     * Handler for the generic control message information
     * from different components in the system.
     */
    Handler<ControlMessageInternal.Response> handlerControlMessageInternalResponse = new Handler<ControlMessageInternal.Response>() {

        @Override
        public void handle(ControlMessageInternal.Response event) {
            handleInternalControlResponse(event);
        }
    };


    /**
     * Simply remove the data in the maps belonging to the address id for the
     *
     * @param sourceAddress Address
     */
    public void cleanControlRequestMessageData(BasicAddress sourceAddress) {

        logger.debug(" {}: Clean Control Message Data Called for: {} ", self.getId(), sourceAddress);
        peerControlRequestAddressIdMap.remove(sourceAddress);
        peerControlResponseMap.remove(sourceAddress);

    }


    /**
     * Handler for the control message responses from the nodes with higher utility in the system.
     */
    ClassMatchedHandler<ControlInformation.Response, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, ControlInformation.Response>> handlerControlMessageResponse = new ClassMatchedHandler<ControlInformation.Response, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, ControlInformation.Response>>() {
        @Override
        public void handle(ControlInformation.Response response, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, ControlInformation.Response> event) {

            logger.debug("{}: Received Control Message from :{}", self.getId(), event.getSource());

            if (!controlMessageExchangeRoundId.equals(response.getRoundId())) {
                logger.warn("{}: Control Message Response received for an expired round: {}", self.getId(), response.getRoundId());
                return;
            }

            ByteBuf buffer = Unpooled.wrappedBuffer(response.getByteInfo());
            try {

                int numUpdates = ControlMessageDecoderFactory.getNumberOfUpdates(buffer);

                while (numUpdates > 0) {

                    ControlBase controlMessageInternalResponse = ControlMessageDecoderFactory.decodeControlMessageInternal(buffer, event.getSource());
                    if (controlMessageInternalResponse != null) {
                        ControlMessageHelper.updateTheControlMessageResponseHolderMap(controlMessageInternalResponse, controlMessageResponseHolderMap);
                    }

                    numUpdates--;
                }

                controlMessageResponseCount++;
                if (controlMessageResponseCount >= config.getIndexExchangeRequestNumber()) {
                    performControlMessageResponseMatching();
                    cleanControlMessageResponseData();
                }

            } catch (MessageDecodingException e) {
                logger.error(" Message Decoding Failed at :" + self.getAddress().getId());
                cleanControlMessageResponseData();
            }
        }
    };

    /**
     * Once all the messages for a round are received, then perform the matching.
     */
    private void performControlMessageResponseMatching() {

        logger.debug("Start with the PeerControl Response Matching at: " + self.getId());

        // Iterate over the keyset and handle specific cases based on your methodology.
        for (Map.Entry<ControlMessageResponseTypeEnum, List<? extends ControlBase>> entry : controlMessageResponseHolderMap.entrySet()) {

            switch (entry.getKey()) {

                case PARTITION_UPDATE_RESPONSE: {
                    logger.debug(" Started with handling of the Partition Update Response ");
                    performPartitionUpdateMatching((List<PartitionControlResponse>) entry.getValue());
                    break;
                }

                case LEADER_UPDATE_RESPONSE: {
                    logger.debug(" Handle Leader Update Response .. ");
                    performLeaderUpdateMatching((List<LeaderInfoControlResponse>) entry.getValue());
                    break;
                }
            }
        }
    }


    private void performLeaderUpdateMatching(List<LeaderInfoControlResponse> leaderControlResponses) {

        DecoratedAddress newLeader = null;
        PublicKey newLeaderPublicKey = null;
        boolean isFirst = true;
        //agree to a leader only if all received responses have leader as null or
        // points to the same exact same leader.
        boolean hasAgreedLeader = true;

        for (LeaderInfoControlResponse leaderInfo : leaderControlResponses) {

            DecoratedAddress currentLeader = leaderInfo.getLeaderAddress();
            PublicKey currentLeaderPublicKey = leaderInfo.getLeaderPublicKey();

            if (isFirst) {
                newLeader = currentLeader;
                newLeaderPublicKey = leaderInfo.getLeaderPublicKey();
                isFirst = false;
            } else {

                if ((currentLeader != null && newLeader == null) ||
                        (newLeader != null && currentLeader == null)) {
                    hasAgreedLeader = false;
                    break;
                } else if (currentLeader != null && newLeader != null &&
                        currentLeaderPublicKey != null && newLeaderPublicKey != null) {
                    if (newLeader.equals(currentLeader) == false ||
                            newLeaderPublicKey.equals(currentLeaderPublicKey) == false) {
                        hasAgreedLeader = false;
                        break;
                    }
                }
            }
        }

        if (hasAgreedLeader) {
            updateLeaderIds(newLeaderPublicKey);
            trigger(new LeaderInfoUpdate(newLeader, newLeaderPublicKey), leaderStatusPort);
        }
    }

    /**
     * Extract the partition updates who's hashes match but sequence should not be violated.
     *
     * @param partitionControlResponses
     */
    private void performPartitionUpdateMatching(List<PartitionControlResponse> partitionControlResponses) {

        Iterator<PartitionControlResponse> iterator = partitionControlResponses.iterator();

        List<java.util.UUID> finalPartitionUpdates = new ArrayList<java.util.UUID>();
        boolean mismatchFound = false;
        boolean first = true;

        ControlMessageEnum baseControlMessageEnum = null;
        List<PartitionHelper.PartitionInfoHash> basePartitioningUpdateHashes = null;

        while (iterator.hasNext()) {

            PartitionControlResponse pcr = iterator.next();

            if (first) {
                // Set the base matching structure.
                baseControlMessageEnum = pcr.getControlMessageEnum();
                basePartitioningUpdateHashes = pcr.getPartitionUpdateHashes();
                first = false;
                continue;
            }

            // Simply match with the base one.
            if (baseControlMessageEnum != pcr.getControlMessageEnum() || !(basePartitioningUpdateHashes.size() > 0)) {
                mismatchFound = true;
                break;
            } else {

                // Check the list of partitioning updates.
                List<PartitionHelper.PartitionInfoHash> currentPartitioningUpdateHashes = pcr.getPartitionUpdateHashes();
                int minimumLength = (basePartitioningUpdateHashes.size() < currentPartitioningUpdateHashes.size()) ? basePartitioningUpdateHashes.size() : currentPartitioningUpdateHashes.size();

                int i = 0;
                while (i < minimumLength) {
                    if (!basePartitioningUpdateHashes.get(i).equals(currentPartitioningUpdateHashes.get(i)))
                        break;
                    i++;
                }

                // If mismatch found and loop didn't run completely.
                if (i < minimumLength) {
                    // Remove the unmatched part.
                    basePartitioningUpdateHashes.subList(i, basePartitioningUpdateHashes.size()).clear();
                }
            }
        }

        if (mismatchFound || !(basePartitioningUpdateHashes.size() > 0)) {
            logger.debug("Not Applying any Partition Update.");
            return;
        }


        for (PartitionHelper.PartitionInfoHash infoHash : basePartitioningUpdateHashes) {
            finalPartitionUpdates.add(infoHash.getPartitionRequestId());
        }

        // Here we have to start a new flow with a different timrout id to fetch the updates from any random node and put it as current
        // request for the updates from any random node.
        Random random = new Random();
        DecoratedAddress randomPeerAddress = partitionControlResponses.get(random.nextInt(partitionControlResponses.size())).getSourceAddress();

        se.sics.kompics.timer.ScheduleTimeout st = new se.sics.kompics.timer.ScheduleTimeout(config.getDelayedPartitioningRequestTimeout());
        st.setTimeoutEvent(new DelayedPartitioning.Timeout(st));
        java.util.UUID roundId = st.getTimeoutEvent().getTimeoutId();

        currentPartitionInfoFetchRound = roundId;
        partitionUpdateFetchInProgress = true;

        // Trigger the new updates.
        DelayedPartitioning.Request request = new DelayedPartitioning.Request(roundId, finalPartitionUpdates);
        trigger(CommonHelper.getDecoratedContentMessage(self.getAddress(), randomPeerAddress, Transport.UDP, request), networkPort);

        // Trigger the Scehdule Timeout Event.
        trigger(st, timerPort);
    }


    /**
     * Handler for the delayed partitioning information request
     * sent by the node which has found common in order hashes.
     */
    ClassMatchedHandler<DelayedPartitioning.Request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, DelayedPartitioning.Request>> handlerDelayedPartitioningRequest = new ClassMatchedHandler<DelayedPartitioning.Request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, DelayedPartitioning.Request>>() {
        @Override
        public void handle(DelayedPartitioning.Request request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, DelayedPartitioning.Request> event) {

            logger.debug("{}: Received delayed partitioning request from : {}", self.getId(), event.getSource());

            LinkedList<PartitionHelper.PartitionInfo> partitionUpdates = fetchPartitioningUpdates(request.getPartitionRequestIds());
            DelayedPartitioning.Response response = new DelayedPartitioning.Response(request.getRoundId(), partitionUpdates);
            trigger(CommonHelper.getDecoratedContentMessage(self.getAddress(), event.getSource(), Transport.UDP, response), networkPort);
        }
    };


    /**
     * Handler for the delayed partitioning information response. The response should contain
     * partitioning history which needs to be applied at the node in order.
     */
    ClassMatchedHandler<DelayedPartitioning.Response, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, DelayedPartitioning.Response>> delayedPartitioningResponseHandler = new ClassMatchedHandler<DelayedPartitioning.Response, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, DelayedPartitioning.Response>>() {
        @Override
        public void handle(DelayedPartitioning.Response response, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, DelayedPartitioning.Response> event) {
            logger.debug("{}: Received delayed partitioning response from: {}", self.getId(), event.getSource());

            if (currentPartitionInfoFetchRound != null && !(response.getRoundId().equals(currentPartitionInfoFetchRound))) {
                logger.warn("{}: Response for the expired delayed partitioning round.");
                return;
            }

            se.sics.kompics.timer.CancelTimeout cancelTimeout = new se.sics.kompics.timer.CancelTimeout(response.getRoundId());
            trigger(cancelTimeout, timerPort);

            // Simply apply the partitioning update and handle the duplicacy.
            applyPartitioningUpdate(response.getPartitionHistory());
        }
    };


    Handler<DelayedPartitioning.Timeout> delayedPartitioningTimeoutHandler = new Handler<DelayedPartitioning.Timeout>() {
        @Override
        public void handle(DelayedPartitioning.Timeout timeout) {
            logger.debug("{}: Delayed partitioning round expired.", self.getId());

            if (!partitionUpdateFetchInProgress)
                currentPartitionInfoFetchRound = null;
        }
    };

    /**
     * After the exchange round is complete or aborted, clean the response data held from precious round.
     */
    private void cleanControlMessageResponseData() {

        //reset the count variable and the map.
        controlMessageResponseCount = 0;
        controlMessageResponseHolderMap.clear();
    }


    /**
     * Issue an index exchange with another node.
     */
    final Handler<TimeoutCollection.ExchangeRound> handleRound = new Handler<TimeoutCollection.ExchangeRound>() {
        @Override
        public void handle(TimeoutCollection.ExchangeRound event) {
            if (exchangeInProgress) {
                return;
            }

            exchangeInProgress = true;

            ScheduleTimeout timeout = new ScheduleTimeout(config.getIndexExchangeTimeout());
            timeout.setTimeoutEvent(new TimeoutCollection.IndexExchangeTimeout(timeout));

            indexExchangeTimeout = timeout.getTimeoutEvent().getTimeoutId();

            trigger(timeout, timerPort);
            collectedHashes.clear();

            Long[] existing = existingEntries.toArray(new Long[existingEntries.size()]);
            trigger(new GradientRoutingPort.IndexHashExchangeRequest(lowestMissingIndexValue, existing,
                    indexExchangeTimeout, config.getIndexExchangeRequestNumber()), gradientRoutingPort);
        }
    };

    /**
     * Handler for the index hash exchange request which
     * signals the start of the index exchange round.
     */
    ClassMatchedHandler<IndexHashExchange.Request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, IndexHashExchange.Request>> handleIndexHashExchangeRequest = new ClassMatchedHandler<IndexHashExchange.Request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, IndexHashExchange.Request>>() {
        @Override
        public void handle(IndexHashExchange.Request request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, IndexHashExchange.Request> event) {
            logger.debug("{}: Received Index Hash Exchange from: {}", self.getId(), event.getSource());

            try {

                List<IndexHash> hashes = new ArrayList<IndexHash>();

                // Search for entries the inquirer is missing
                long lastId = request.getLowestMissingIndexEntry();
                for (long i : request.getEntries()) {
                    Collection<IndexEntry> indexEntries = ApplicationLuceneQueries.findIdRange(writeLuceneAdaptor, lastId, i - 1, config.getMaxExchangeCount() - hashes.size());
                    for (IndexEntry indexEntry : indexEntries) {
                        hashes.add((new IndexHash(indexEntry)));
                    }
                    lastId = i + 1;

                    if (hashes.size() >= config.getMaxExchangeCount()) {
                        break;
                    }
                }

                // In case there is some space left search for more
                if (hashes.size() < config.getMaxExchangeCount()) {
                    Collection<IndexEntry> indexEntries = ApplicationLuceneQueries.findIdRange(writeLuceneAdaptor, lastId, Long.MAX_VALUE, config.getMaxExchangeCount() - hashes.size());
                    for (IndexEntry indexEntry : indexEntries) {
                        hashes.add((new IndexHash(indexEntry)));
                    }
                }

                IndexHashExchange.Response response = new IndexHashExchange.Response(request.getExchangeRoundId(), hashes);
                trigger(CommonHelper.getDecoratedContentMessage(self.getAddress(), event.getSource(), Transport.UDP, response), networkPort);

            } catch (IOException e) {
                logger.error(self.getId() + " " + e.getMessage());
            }


        }
    };

    ClassMatchedHandler<IndexHashExchange.Response, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, IndexHashExchange.Response>> handleIndexHashExchangeResponse = new ClassMatchedHandler<IndexHashExchange.Response, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, IndexHashExchange.Response>>() {
        @Override
        public void handle(IndexHashExchange.Response response, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, IndexHashExchange.Response> event) {

            logger.debug("{}: Received index hash exchange response from the node: {}", self.getId(), event.getSource());

            // Drop old responses
            if (!response.getExchangeRoundId().equals(indexExchangeTimeout)) {
                logger.warn("{}: Received response for an old index hash exchange request.", self.getId());
                return;
            }
            
//            nodesRespondedInIndexHashExchange.add(event.getSource());

            // TODO we somehow need to check here that the answer is from the correct node
            collectedHashes.put(event.getSource(), response.getIndexHashes());
            if (collectedHashes.size() == config.getIndexExchangeRequestNumber()) {
                intersection = new HashSet<IndexHash>(collectedHashes.values().iterator().next());
                for (Collection<IndexHash> hashes : collectedHashes.values()) {
                    intersection.retainAll(hashes);
                }

                if (intersection.isEmpty()) {
                    CancelTimeout cancelTimeout = new CancelTimeout(indexExchangeTimeout);
                    trigger(cancelTimeout, timerPort);
                    resetExchangeParameters();
                    return;
                }

                ArrayList<Id> ids = new ArrayList<Id>();
                for (IndexHash hash : intersection) {
                    ids.add(hash.getId());
                }

                // Use Softmax approach to select the node to ask the request for index entries from.
                DecoratedAddress node = collectedHashes.keySet().iterator().next();
                IndexExchange.Request request = new IndexExchange.Request(response.getExchangeRoundId(), ids);
                trigger(CommonHelper.getDecoratedContentMessage(self.getAddress(), node, Transport.UDP, request), networkPort);
                
            }
        }
    };


    /**
     * Handle the index exchange request by supplied the index entries for the 
     * requested ids by the node lower in the view.
     */
    ClassMatchedHandler<IndexExchange.Request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, IndexExchange.Request>> handleIndexExchangeRequest = new ClassMatchedHandler<IndexExchange.Request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, IndexExchange.Request>>() {
        @Override
        public void handle(IndexExchange.Request request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, IndexExchange.Request> event) {
            
            logger.debug("{}: Received Index Exchange request from: {}", self.getId(), event.getSource());
            
            try {
                List<IndexEntry> indexEntries = new ArrayList<IndexEntry>();
                for (Id id : request.getIds()) {
                    
                    IndexEntry entry = findById(id.getId());
                    if (entry != null)
                        indexEntries.add(entry);
                }
                
                IndexExchange.Response  response = new IndexExchange.Response(request.getExchangeRoundId(), indexEntries, 0, 0);
                trigger(CommonHelper.getDecoratedContentMessage(self.getAddress(), event.getSource(), Transport.UDP, response), networkPort);
                
            } catch (IOException e) {
                logger.error(self.getId() + " " + e.getMessage());
            }
        }
    };


    /**
     * Reset the exchange parameters.
     */
    private void resetExchangeParameters(){
        
        indexExchangeTimeout = null;
        exchangeInProgress = false;
    }
    
    
    ClassMatchedHandler<IndexExchange.Response, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, IndexExchange.Response>> handleIndexExchangeResponse = new ClassMatchedHandler<IndexExchange.Response, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, IndexExchange.Response>>() {
        @Override
        public void handle(IndexExchange.Response response, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, IndexExchange.Response> event) {
            
            logger.debug("{}: Received index exchange response from the node: {}", self.getId(), event.getSource());
            // Drop old responses
            if (!response.getExchangeRoundId().equals(indexExchangeTimeout)) {
                return;
            }

            // Extra informtion for restating the check.
//            // Stop accepting responses from lagging behind nodes.
//            if (isMessageFromNodeLaggingBehind(event.getVodSource())) {
//                return;
//            }


            CancelTimeout cancelTimeout = new CancelTimeout(indexExchangeTimeout);
            trigger(cancelTimeout, timerPort);
            resetExchangeParameters();

            try {
                for (IndexEntry indexEntry : response.getIndexEntries()) {
                    if (intersection.remove(new IndexHash(indexEntry)) && ApplicationSecurity.isIndexEntrySignatureValid(indexEntry)) {
                        addEntryLocal(indexEntry);
                    } else {
                        logger.warn("Unable to process Index Entry fetched via Index Hash Exchange.");
                    }
                }
            } catch (IOException e) {
                logger.error(self.getId() + " " + e.getMessage());
            } catch (LuceneAdaptorException e) {
                e.printStackTrace();
            }
        }
    };

    /**
     * Index Exchange Timeout Collection.
     */
    final Handler<TimeoutCollection.IndexExchangeTimeout> handleIndexExchangeTimeout = new Handler<TimeoutCollection.IndexExchangeTimeout>() {
        @Override
        public void handle(TimeoutCollection.IndexExchangeTimeout event) {
            
            logger.debug(self.getId() + " index exchange timed out");
            
            if(indexExchangeTimeout != null && indexExchangeTimeout.equals(event.getTimeoutId())){
                resetExchangeParameters();
            }
            else{
                logger.warn("{}: Timeout triggered when already round reset or moved ahead");
            }
        }
    };

    /**
     * Add index entries for the simulator.
     */
    final Handler<AddIndexSimulated> handleAddIndexSimulated = new Handler<AddIndexSimulated>() {
        @Override
        public void handle(AddIndexSimulated event) {
            addEntryGlobal(event.getEntry());
        }
    };

    final Handler<SimulationEventsPort.SearchSimulated> handleSearchSimulated = new Handler<SimulationEventsPort.SearchSimulated>() {
        @Override
        public void handle(SimulationEventsPort.SearchSimulated event) {
            startSearch(event.getSearchPattern());
        }
    };

    /**
     * Add a new {@link IndexEntry} to the system and schedule a timeout
     * to wait for the acknowledgment.
     *
     * @param entry the {@link IndexEntry} to be added
     */
    private void addEntryGlobal(IndexEntry entry) {

        ScheduleTimeout rst = new ScheduleTimeout(config.getAddTimeout());
        rst.setTimeoutEvent(new AddIndexTimeout(rst, config.getRetryCount(), entry));
        addEntryGlobal(entry, rst);
    }

    /**
     * Add a new {@link IndexEntry} to the system, add the given timeout to the timer.
     *
     * @param entry   the {@link IndexEntry} to be added
     * @param timeout timeout for adding the entry
     */
    private void addEntryGlobal(IndexEntry entry, ScheduleTimeout timeout) {

        trigger(timeout, timerPort);
        trigger(new GradientRoutingPort.AddIndexEntryRequest(entry, timeout.getTimeoutEvent().getTimeoutId()), gradientRoutingPort);
        timeStoringMap.put(timeout.getTimeoutEvent().getTimeoutId(), (new Date()).getTime());
    }


    /**
     * Handler executed in the role of the leader. Create a new id and search
     * for a the according bucket in the routing table. If it does not include
     * enough nodes to satisfy the replication requirements then create a new id
     * and try again. Send a {@link se.sics.ms.messages.ReplicationPrepareCommitMessage} request to a number of nodes as
     * specified in the config file and schedule a timeout to wait for
     * responses. The adding operation will be acknowledged if either all nodes
     * responded to the {@link se.sics.ms.messages.ReplicationPrepareCommitMessage} request or the timeout occurred and
     * enough nodes, as specified in the config, responded.
     */

    ClassMatchedHandler<AddIndexEntry.Request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, AddIndexEntry.Request>> handleAddIndexEntryRequest = new ClassMatchedHandler<AddIndexEntry.Request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, AddIndexEntry.Request>>() {
        @Override
        public void handle(AddIndexEntry.Request request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, AddIndexEntry.Request> event) {

            logger.debug("{}: Received add index entry request from : {}", self.getId(),  event.getSource());
            if (!leader || partitionInProgress) {
                logger.warn("{}: Received request to add entry but self state doesn't permit to move ahead. Returning ... ");
                return;
            }

            if (!entryAdditionTracker.canTrack()) {
                logger.warn("{}: Unable to track a new entry addition limit reached ... ");
                return;
            }

            // FIX : The timeout for clearing the map is way too low
            if (recentRequests.containsKey(request.getEntryAdditionRound())) {
                return;
            }

//            Snapshot.incrementReceivedAddRequests();
            recentRequests.put(request.getEntryAdditionRound(), System.currentTimeMillis());

            IndexEntry newEntry = request.getEntry();
            long id = getNextInsertionId();

            newEntry.setId(id);
            newEntry.setLeaderId(publicKey);
            newEntry.setGlobalId(java.util.UUID.randomUUID().toString());
            String signature = ApplicationSecurity.generateSignedHash(newEntry, privateKey);

            if (signature == null) {
                logger.warn("Unable to generate the hash for the index entry with id: {}", newEntry.getId());
                return;
            }

            newEntry.setHash(signature);
            if (leaderGroupInformation != null && !leaderGroupInformation.isEmpty()) {

                logger.warn("Started tracking for the entry addition with id: {} for address: {}", newEntry.getId(), event.getSource());
                EntryAdditionRoundInfo additionRoundInfo = new EntryAdditionRoundInfo(request.getEntryAdditionRound(), leaderGroupInformation, newEntry, event.getSource());
                entryAdditionTracker.startTracking(request.getEntryAdditionRound(), additionRoundInfo);

                ReplicationPrepareCommit.Request  prepareRequest = new ReplicationPrepareCommit.Request(newEntry, request.getEntryAdditionRound());
                for (DecoratedAddress destination : leaderGroupInformation) {
                    logger.warn("Sending prepare commit request to : {}", destination.getId());
                    trigger(CommonHelper.getDecoratedContentMessage(self.getAddress(), destination, Transport.UDP, prepareRequest), networkPort);
                }

                // Trigger for a timeout and how would that work ?
                ScheduleTimeout st = new ScheduleTimeout(5000);
                st.setTimeoutEvent(new TimeoutCollection.EntryPrepareResponseTimeout(st, request.getEntryAdditionRound()));
                entryPrepareTimeoutMap.put(request.getEntryAdditionRound(), st.getTimeoutEvent().getTimeoutId());
                trigger(st, timerPort);

            } else {
                logger.warn("{}: Unable to start the index entry commit due to insufficient information about leader group.", self.getId());
            }
        }
    };

    /**
     * The entry addition prepare phase timed out and I didn't receive all the responses from the leader group nodes.
     * Reset the tracker but also keep track of the edge case.
     */
    Handler<TimeoutCollection.EntryPrepareResponseTimeout> preparePhaseTimeout = new Handler<TimeoutCollection.EntryPrepareResponseTimeout>() {
        @Override
        public void handle(TimeoutCollection.EntryPrepareResponseTimeout event) {

            if (entryPrepareTimeoutMap != null && entryPrepareTimeoutMap.containsValue(event.getTimeoutId())) {

                logger.warn("{}: Prepare phase timed out. Resetting the tracker information.");
                entryAdditionTracker.resetTracker(event.getEntryAdditionRoundId());
                entryPrepareTimeoutMap.remove(event.getEntryAdditionRoundId());

            } else {
                logger.warn(" Prepare Phase timeout edge case called. Not resetting the tracker.");
            }

        }
    };


    /*
     * @return a new id for a new {@link IndexEntry}
     */
    private long getNextInsertionId() {
        if (nextInsertionId == Long.MAX_VALUE - 1)
            nextInsertionId = Long.MIN_VALUE;

        return nextInsertionId++;
    }

    /**
     * No acknowledgment for an issued {@link AddIndexEntryMessage.Request} was received
     * in time. Try to add the entry again or respons with failure to the web client.
     */
    final Handler<AddIndexTimeout> handleAddRequestTimeout = new Handler<AddIndexTimeout>() {
        @Override
        public void handle(AddIndexTimeout event) {

            timeStoringMap.remove(event.getTimeoutId());

            if (event.reachedRetryLimit()) {
                logger.warn("{} reached retry limit for adding a new entry {} ", self.getAddress(), event.entry);
                trigger(new UiAddIndexEntryResponse(false), uiPort);
            }
            else {

                //If prepare phase was started but no response received, then replicationRequests will have left
                // over data
                replicationRequests.remove(event.getTimeoutId());

                event.incrementTries();
                ScheduleTimeout rst = new ScheduleTimeout(config.getAddTimeout());
                rst.setTimeoutEvent(event);
                addEntryGlobal(event.getEntry(), rst);
            }
        }
    };


    /**
     * The request depicting leader request for the promises from the nodes present in the leader group.
     * The request needs to be validated that it is from the correct leader and then added to maping depicting pending for commit.
     */
    ClassMatchedHandler<ReplicationPrepareCommit.Request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, ReplicationPrepareCommit.Request>> handlePrepareCommit  = new ClassMatchedHandler<ReplicationPrepareCommit.Request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, ReplicationPrepareCommit.Request>>() {
        @Override
        public void handle(ReplicationPrepareCommit.Request request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, ReplicationPrepareCommit.Request> event) {

            logger.debug("{}: Received Index Entry prepare request from the node: {}", self.getId(), event.getSource());

            IndexEntry entry = request.getEntry();
            if (!ApplicationSecurity.isIndexEntrySignatureValid(entry) || !leaderIds.contains(entry.getLeaderId()))
                return;

            ReplicationPrepareCommit.Response response = new ReplicationPrepareCommit.Response(request.getIndexAdditionRoundId(), request.getEntry().getId());
            trigger(CommonHelper.getDecoratedContentMessage(self.getAddress(), event.getSource(), Transport.UDP, response), networkPort);

            se.sics.kompics.timer.ScheduleTimeout st = new se.sics.kompics.timer.ScheduleTimeout(config.getReplicationTimeout());
            st.setTimeoutEvent(new AwaitingForCommitTimeout(st, request.getEntry()));
            st.getTimeoutEvent().getTimeoutId();

            pendingForCommit.put(request.getEntry(), st.getTimeoutEvent().getTimeoutId());
            trigger(st, timerPort);

        }
    };

    /**
     * The promise for the index entry addition expired and therefore the entry needs to be removed from the map.
     *
     */
    final Handler<AwaitingForCommitTimeout> handleAwaitingForCommitTimeout = new Handler<AwaitingForCommitTimeout>() {
        @Override
        public void handle(AwaitingForCommitTimeout awaitingForCommitTimeout) {

            logger.warn("{}: Index entry prepare phase timed out. Reset the map information.");
            if (pendingForCommit.containsKey(awaitingForCommitTimeout.getEntry()))
                pendingForCommit.remove(awaitingForCommitTimeout.getEntry());
        }
    };


    /**
     * Prepare Commit Message from the peers in the system. Update the tracker and check if all the nodes have replied and
     * then send the commit message request to the leader nodes who have replied yes.
     */
    ClassMatchedHandler<ReplicationPrepareCommit.Response, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, ReplicationPrepareCommit.Response>> handleEntryAdditionPrepareCommitResponse = new ClassMatchedHandler<ReplicationPrepareCommit.Response, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, ReplicationPrepareCommit.Response>>() {
        @Override
        public void handle(ReplicationPrepareCommit.Response response, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, ReplicationPrepareCommit.Response> event) {

            logger.debug("{}: Received Index entry prepare response from:{}", self.getId(), event.getSource());

            java.util.UUID entryAdditionRoundId = response.getIndexAdditionRoundId();
            EntryAdditionRoundInfo info = entryAdditionTracker.getEntryAdditionRoundInfo(entryAdditionRoundId);

            if (info == null) {
                logger.debug("{}: Received Promise Response from: {} after the round has expired ", self.getId(), event.getSource());
                return;
            }

            info.addEntryAddPromiseResponse(response);
            if (info.isPromiseAccepted()) {

                try {

                    logger.warn("{}: All nodes have promised for entry addition. Move to commit. ", self.getId());
                    CancelTimeout ct = new CancelTimeout(entryPrepareTimeoutMap.get(entryAdditionRoundId));
                    trigger(ct, timerPort);

                    IndexEntry entryToCommit = info.getEntryToAdd();
                    UUID commitTimeout = UUID.randomUUID(); //What's it purpose.
                    addEntryLocal(entryToCommit);   // Commit to local first.

                    ByteBuffer idBuffer = ByteBuffer.allocate(8);
                    idBuffer.putLong(entryToCommit.getId());

                    String signature = ApplicationSecurity.generateRSASignature(idBuffer.array(), privateKey);

                    ReplicationCommit.Request commitRequest = new ReplicationCommit.Request(commitTimeout, entryToCommit.getId(), signature);
                    for (DecoratedAddress destination : info.getLeaderGroupAddress()) {
                        trigger(CommonHelper.getDecoratedContentMessage(self.getAddress(), destination, Transport.UDP, commitRequest), networkPort);
                    }
                    
                    // Send reply to the originator node. ( Not actually two phase commit as I assume that they will have added entries. )
                    AddIndexEntry.Response addEntryResponse = new AddIndexEntry.Response(info.getEntryAdditionRoundId());
                    trigger(CommonHelper.getDecoratedContentMessage(self.getAddress(), info.getEntryAddSourceNode(),Transport.UDP, addEntryResponse), networkPort);

                } catch (NoSuchAlgorithmException e) {
                    logger.error(self.getId() + " " + e.getMessage());
                } catch (InvalidKeyException e) {
                    logger.error(self.getId() + " " + e.getMessage());
                } catch (SignatureException e) {
                    logger.error(self.getId() + " " + e.getMessage());
                } catch (LuceneAdaptorException e) {
                    e.printStackTrace();
                    throw new RuntimeException("Entry addition failed", e);
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {

                    entryAdditionTracker.resetTracker(entryAdditionRoundId);
                    entryPrepareTimeoutMap.remove(entryAdditionRoundId);
                }
            }
        }
    };


    /**
     * Handler for the entry commit request as part of the index entry addition protocol.
     * Verify that the request is from the leader and then add the entry to the node.
     *
     * <b>CAUTION :</b> Currently we are not replying to the node back and simply without any questioning add the entry
     * locally, simply the verifying the signature.
     */
    ClassMatchedHandler<ReplicationCommit.Request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>,ReplicationCommit.Request>> handleEntryCommitRequest = new ClassMatchedHandler<ReplicationCommit.Request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, ReplicationCommit.Request>>() {
        @Override
        public void handle(ReplicationCommit.Request request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, ReplicationCommit.Request> event) {

            logger.debug("{}: Received index entry commit request from : {}", self.getId(), event.getSource());
            long id = request.getEntryId();

            if (leaderIds.isEmpty())
                return;

            ByteBuffer idBuffer = ByteBuffer.allocate(8);
            idBuffer.putLong(id);
            try {
                if (!ApplicationSecurity.verifyRSASignature(idBuffer.array(), leaderIds.get(leaderIds.size() - 1), request.getSignature()))
                    return;
            } catch (NoSuchAlgorithmException e) {
                logger.error(self.getId() + " " + e.getMessage());
            } catch (InvalidKeyException e) {
                logger.error(self.getId() + " " + e.getMessage());
            } catch (SignatureException e) {
                logger.error(self.getId() + " " + e.getMessage());
            }

            IndexEntry toCommit = null;
            for (IndexEntry entry : pendingForCommit.keySet()) {
                if (entry.getId() == id) {
                    toCommit = entry;
                    break;
                }
            }

            if (toCommit == null){
                logger.warn("{}: Unable to find index entry with id: {} to commit.", self.getId(), id);
                return;
            }

            se.sics.kompics.timer.CancelTimeout ct = new se.sics.kompics.timer.CancelTimeout(pendingForCommit.get(toCommit));
            trigger(ct, timerPort);

            try {

                addEntryLocal(toCommit);
                pendingForCommit.remove(toCommit);
                long maxStoredId = getMaxStoredId();

                ArrayList<Long> missingIds = new ArrayList<Long>();
                long currentMissingValue = maxStoredId < 0 ? 0 : maxStoredId + 1;
                while (currentMissingValue < toCommit.getId()) {
                    missingIds.add(currentMissingValue);
                    currentMissingValue++;
                }

                if (missingIds.size() > 0) {
                    Repair.Request repairRequest = new Repair.Request(request.getCommitRoundId(), missingIds.toArray(new Long[missingIds.size()]));     // FIXME: Repair Message has no associated round.
                    trigger(CommonHelper.getDecoratedContentMessage(self.getAddress(), event.getSource(), Transport.UDP, repairRequest), networkPort);
                }
            } catch (IOException e) {
                logger.error(self.getId() + " " + e.getMessage());
            } catch (LuceneAdaptorException e) {
                e.printStackTrace();
            }

        }
    };




    ClassMatchedHandler<AddIndexEntry.Response, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, AddIndexEntry.Response>> handleAddIndexEntryResponse = new ClassMatchedHandler<AddIndexEntry.Response, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, AddIndexEntry.Response>>(){
        @Override
        public void handle(AddIndexEntry.Response response, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, AddIndexEntry.Response> event) {

            logger.debug("{}: Received add index entry response back:", self.getId());
            CancelTimeout ct = new CancelTimeout(response.getEntryAdditionRound());
            trigger(ct, timerPort);

            Long timeStarted = timeStoringMap.get(response.getEntryAdditionRound());
            timeStoringMap.remove(response.getEntryAdditionRound());
            trigger(new UiAddIndexEntryResponse(true), uiPort);

        }
    };


    /**
     * Returns max stored id on a peer
     * @return max stored id on a peer
     */
    private long getMaxStoredId() {
        long currentIndexValue = lowestMissingIndexValue - 1;

        if (existingEntries.isEmpty() || currentIndexValue > Collections.max(existingEntries))
            return currentIndexValue;

        return Collections.max(existingEntries);
    }


    /**
     * Handles situations regarding a peer in the leader group is behind in the updates during add operation
     * and asks for missing data
     */
    ClassMatchedHandler<Repair.Request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, Repair.Request>> handleRepairRequest = new ClassMatchedHandler<Repair.Request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, Repair.Request>>() {
        @Override
        public void handle(Repair.Request request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, Repair.Request> event) {

            logger.debug("{}: Received repair request from the node: {}", self.getId(), event.getSource());
            ArrayList<IndexEntry> missingEntries = new ArrayList<IndexEntry>();
            try {
                for (int i = 0; i < request.getMissingIds().length; i++) {
                    IndexEntry entry = findById(request.getMissingIds()[i]);
                    if (entry != null) missingEntries.add(entry);
                }
            } catch (IOException e) {
                logger.error(self.getId() + " " + e.getMessage());
            }

            Repair.Response msg = new Repair.Response(request.getRepairRoundId(), missingEntries);
            trigger(CommonHelper.getDecoratedContentMessage(self.getAddress(), event.getSource(), Transport.UDP, msg), networkPort);
        }
    };


    /**
     * Handles missing data on the peer from the leader group when adding a new entry, but the peer is behind
     * with the updates
     */
    ClassMatchedHandler<Repair.Response, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, Repair.Response>> handleRepairResponse = new ClassMatchedHandler<Repair.Response, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, Repair.Response>>() {
        @Override
        public void handle(Repair.Response response, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, Repair.Response> event) {

            logger.debug("{}: Received Repair response from: {}", self.getId(), event.getSource());
            try {
                for (IndexEntry entry : response.getMissingEntries())
                    if (entry != null) addEntryLocal(entry);
            } catch (IOException e) {
                logger.error(self.getId() + " " + e.getMessage());
            } catch (LuceneAdaptorException e) {
                e.printStackTrace();
            }
        }
    };

    /**
     * Periodically garbage collect the data structure used to identify
     * duplicated {@link AddIndexEntryMessage.Request}.
     */
    final Handler<TimeoutCollection.RecentRequestsGcTimeout> handleRecentRequestsGcTimeout = new Handler<TimeoutCollection.RecentRequestsGcTimeout>() {
        @Override
        public void handle(TimeoutCollection.RecentRequestsGcTimeout event) {
            long referenceTime = System.currentTimeMillis();

            ArrayList<UUID> removeList = new ArrayList<UUID>();
            for (UUID id : recentRequests.keySet()) {
                if (referenceTime - recentRequests.get(id) > config.getRecentRequestsGcInterval()) {
                    removeList.add(id);
                }
            }

            for (UUID uuid : removeList) {
                recentRequests.remove(uuid);
            }
        }
    };

    public void updateLeaderIds(PublicKey newLeaderPublicKey) {

        if (newLeaderPublicKey != null) {
            if (!leaderIds.contains(newLeaderPublicKey)) {
                if (leaderIds.size() == config.getMaxLeaderIdHistorySize())
                    leaderIds.remove(leaderIds.get(0));
                leaderIds.add(newLeaderPublicKey);
            } else {
                //if leader already exists in the list, move it to the top
                leaderIds.remove(newLeaderPublicKey);
                leaderIds.add(newLeaderPublicKey);
            }
        }
    }

    final Handler<UiSearchRequest> searchRequestHandler = new Handler<UiSearchRequest>() {
        @Override
        public void handle(UiSearchRequest searchRequest) {
            startSearch(searchRequest.getPattern());
        }
    };

    final Handler<UiAddIndexEntryRequest> addIndexEntryRequestHandler = new Handler<UiAddIndexEntryRequest>() {
        @Override
        public void handle(UiAddIndexEntryRequest addIndexEntryRequest) {
            addEntryGlobal(addIndexEntryRequest.getEntry());
        }
    };

    final Handler<NumberOfPartitions> handleNumberOfPartitions = new Handler<NumberOfPartitions>() {
        @Override
        public void handle(NumberOfPartitions numberOfPartitions) {
            searchPartitionsNumber.put(numberOfPartitions.getTimeoutId(), numberOfPartitions.getNumberOfPartitions());
            searchRequestStarted.put(numberOfPartitions.getTimeoutId(), new Pair<Long, Integer>(System.currentTimeMillis(),
                    numberOfPartitions.getNumberOfPartitions()));
        }
    };

    /**
     * Send a search request for a given search pattern to one node in each
     * partition except the local partition.
     *
     * @param pattern the search pattern
     */
    private void startSearch(SearchPattern pattern) {

        // TODO: Add check for the same request but a different page ( Implement Pagination ).
        searchRequest = new LocalSearchRequest(pattern);
        closeIndex(searchIndex);

        searchIndex = new RAMDirectory();
        searchRequestLuceneAdaptor = new LuceneAdaptorImpl(searchIndex, indexWriterConfig);

        try {
            searchRequestLuceneAdaptor.initialEmptyWriterCommit();
        } catch (LuceneAdaptorException e) {
            e.printStackTrace();
            throw new RuntimeException("Unable to open search index", e);
        }

        ScheduleTimeout rst = new ScheduleTimeout(config.getQueryTimeout());
        rst.setTimeoutEvent(new TimeoutCollection.SearchTimeout(rst));
        searchRequest.setTimeoutId(rst.getTimeoutEvent().getTimeoutId());

        trigger(rst, timerPort);
        trigger(new GradientRoutingPort.SearchRequest(pattern, searchRequest.getTimeoutId(), config.getQueryTimeout()), gradientRoutingPort);
    }


    /**
     * Close opened indexes to prevent memory leak.
     *
     * @param index Index to close.
     */
    private void closeIndex(Directory index) {

        if (index != null) {
            logger.info("Closing previous opened search index to prevent memory leak.");
            try {
                index.close();
            } catch (IOException e) {
                logger.warn(" Unable to close previous search index.");
                e.printStackTrace();
            }
        }
    }



    ClassMatchedHandler<SearchInfo.Request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, SearchInfo.Request>> handleSearchRequest = new ClassMatchedHandler<SearchInfo.Request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, SearchInfo.Request>>() {
        @Override
        public void handle(SearchInfo.Request request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, SearchInfo.Request> event) {

            logger.debug("{}: Received Search Request from : {}", self.getId(), event.getSource());
            try {

                ArrayList<IndexEntry> result = searchLocal(writeLuceneAdaptor, request.getPattern(), config.getHitsPerQuery());
                SearchInfo.Response searchMessageResponse = new SearchInfo.Response(request.getRequestId(), result, request.getPartitionId(), 0, 0);
                trigger(CommonHelper.getDecoratedContentMessage(self.getAddress(), event.getSource(), Transport.UDP, searchMessageResponse), networkPort);

            }
             catch (LuceneAdaptorException e) {
                logger.warn("{} : Unable to search for index entries in Lucene.", self.getId());
                e.printStackTrace();
            }


        }
    };


    /**
     * Query the given index store with a given search pattern.
     *
     * @param adaptor adaptor to use
     * @param pattern the {@link SearchPattern} to use
     * @param limit   the maximal amount of entries to return
     * @return a list of matching entries
     * @throws IOException if Lucene errors occur
     */
    private ArrayList<IndexEntry> searchLocal(LuceneAdaptor adaptor, SearchPattern pattern, int limit) throws LuceneAdaptorException {
        TopScoreDocCollector collector = TopScoreDocCollector.create(limit, true);
        ArrayList<IndexEntry> result = (ArrayList<IndexEntry>) adaptor.searchIndexEntriesInLucene(pattern.getQuery(), collector);
        return result;
    }


    /**
     * Node received search response for the current search request.
     */
    ClassMatchedHandler<SearchInfo.Response, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, SearchInfo.Response>> handleSearchResponse = new ClassMatchedHandler<SearchInfo.Response, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, SearchInfo.Response>>() {
        @Override
        public void handle(SearchInfo.Response response, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, SearchInfo.Response> event) {

            if (searchRequest == null || !response.getSearchTimeoutId().equals(searchRequest.getTimeoutId())) {
                return;
            }
            addSearchResponse(response.getResults(), response.getPartitionId(), response.getSearchTimeoutId());
        }
    };

    /**
     * Add all entries from a {@link SearchMessage.Response} to the search index.
     *
     * @param entries   the entries to be added
     * @param partition the partition from which the entries originate from
     */
    private void addSearchResponse(Collection<IndexEntry> entries, int partition, java.util.UUID requestId) {
        if (searchRequest.hasResponded(partition)) {
            return;
        }

        try {
            addIndexEntries(searchRequestLuceneAdaptor, entries);
        } catch (IOException e) {
            java.util.logging.Logger.getLogger(Search.class.getName()).log(Level.SEVERE, null, e);
        }

        searchRequest.addRespondedPartition(partition);

        Integer numOfPartitions = searchPartitionsNumber.get(requestId);
        if (numOfPartitions == null) {
            return;
        }

        if (searchRequest.getNumberOfRespondedPartitions() == numOfPartitions) {
            logSearchTimeResults(requestId, System.currentTimeMillis(), numOfPartitions);
            se.sics.kompics.timer.CancelTimeout ct = new se.sics.kompics.timer.CancelTimeout(searchRequest.getTimeoutId());
            trigger(ct, timerPort);
            answerSearchRequest();
        }
    }

    private void logSearchTimeResults(java.util.UUID requestId, long timeCompleted, Integer numOfPartitions) {
        Pair<Long, Integer> searchIssued = searchRequestStarted.get(requestId);
        if (searchIssued == null)
            return;

        if (searchIssued.getSecond() != numOfPartitions)
            logger.info(String.format("Search completed in %s ms, hit %s out of %s partitions",
                    config.getQueryTimeout(), numOfPartitions, searchIssued.getSecond()));
        else
            logger.info(String.format("Search completed in %s ms, hit %s out of %s partitions",
                    timeCompleted - searchIssued.getFirst(), numOfPartitions, searchIssued.getSecond()));

        searchRequestStarted.remove(requestId);
    }

    /**
     * Answer a search request if the timeout occurred before all answers were
     * collected.
     */
    final Handler<TimeoutCollection.SearchTimeout> handleSearchTimeout = new Handler<TimeoutCollection.SearchTimeout>() {
        @Override
        public void handle(TimeoutCollection.SearchTimeout event) {
            logSearchTimeResults(event.getTimeoutId(), System.currentTimeMillis(),
                    searchRequest.getNumberOfRespondedPartitions());
            answerSearchRequest();
        }
    };


    /**
     * FIXME: Fix the semantics in terms of error handling.
     * Removes IndexEntries that don't belong to your partition after a partition splits into two
     */
    void removeEntriesNotFromYourPartition(long middleId, boolean isPartition) {

        se.sics.kompics.timer.CancelTimeout cancelTimeout = new se.sics.kompics.timer.CancelTimeout(indexExchangeTimeout);
        trigger(cancelTimeout, timerPort);
        indexExchangeTimeout = null;
        exchangeInProgress = false;

        int numberOfStoredIndexEntries = 0;
        try {

            if (isPartition) {
                ApplicationLuceneQueries.deleteDocumentsWithIdMoreThen(writeLuceneAdaptor, middleId, minStoredId, maxStoredId);
                ApplicationLuceneQueries.deleteHigherExistingEntries(middleId, existingEntries, false);
            } else {
                ApplicationLuceneQueries.deleteDocumentsWithIdLessThen(writeLuceneAdaptor, middleId, minStoredId, maxStoredId);
                ApplicationLuceneQueries.deleteLowerExistingEntries(middleId, existingEntries, true);
            }

            minStoredId = ApplicationLuceneQueries.getMinStoredIdFromLucene(writeLuceneAdaptor);
            maxStoredId = ApplicationLuceneQueries.getMaxStoredIdFromLucene(writeLuceneAdaptor);

            //Increment Max Store Id to keep in line with the original methodology.
            maxStoredId += 1;

            // Update the number of entries in the system.
            numberOfStoredIndexEntries = getTotalStoredEntriesCount(writeLuceneAdaptor);

        } catch (LuceneAdaptorException e) {
            logger.error("{}: Unable to cleanly remove the entries from the partition.", self.getId());
            e.printStackTrace();
        }
        self.setNumberOfEntries(numberOfStoredIndexEntries);


        if (maxStoredId < minStoredId) {
            long temp = maxStoredId;
            maxStoredId = minStoredId;
            minStoredId = temp;
        }

        // TODO: The behavior of the lowestMissingIndex in case of the wrap around needs to be tested and some edge cases exists in this implementation.
        // FIXME: More cleaner solution is required.
        nextInsertionId = maxStoredId;
        lowestMissingIndexValue = (lowestMissingIndexValue < maxStoredId && lowestMissingIndexValue > minStoredId) ? lowestMissingIndexValue : maxStoredId;
        partitionInProgress = false;

    }


    /**
     * Add the given {@link IndexEntry}s to the given Lucene directory
     *
     * @param searchRequestLuceneAdaptor adaptor
     * @param entries                    a collection of index entries to be added
     * @throws IOException in case the adding operation failed
     */
    private void addIndexEntries(LuceneAdaptor searchRequestLuceneAdaptor, Collection<IndexEntry> entries)
            throws IOException {
        try {
            for (IndexEntry entry : entries) {
                addIndexEntry(searchRequestLuceneAdaptor, entry);
            }
        } catch (LuceneAdaptorException e) {
            logger.warn("{}: Unable to update search index with additional entries", self.getId());
            e.printStackTrace();
        }
    }

    private void answerSearchRequest() {
        ArrayList<IndexEntry> result = null;
        try {
            result = searchLocal(searchRequestLuceneAdaptor, searchRequest.getSearchPattern(), config.getMaxSearchResults());
            logger.warn("{} found {} entries for {}", new Object[]{self.getId(), result.size(), searchRequest.getSearchPattern()});

        } catch (LuceneAdaptorException e) {
            result = new ArrayList<IndexEntry>();  // In case of error set the result set as empty.
            logger.warn("{} : Unable to search for the entries.", self.getId());
            e.printStackTrace();
        } finally {
            searchRequest = null;   // Stop handling more searches.
            trigger(new UiSearchResponse(result), uiPort);
        }
    }

    /**
     * Add the given {@link IndexEntry} to the Lucene index using the given
     * writer.
     *
     * @param adaptor the adaptor used to add the {@link IndexEntry}
     * @param entry   the {@link IndexEntry} to be added
     * @throws IOException in case the adding operation failed
     */
    private void addIndexEntry(LuceneAdaptor adaptor, IndexEntry entry) throws IOException, LuceneAdaptorException {

        logger.trace("{}: Adding entry in the system: {}", self.getId(), entry.getId());
        Document doc = new Document();
        doc.add(new StringField(IndexEntry.GLOBAL_ID, entry.getGlobalId(), Field.Store.YES));
        doc.add(new LongField(IndexEntry.ID, entry.getId(), Field.Store.YES));
        doc.add(new StoredField(IndexEntry.URL, entry.getUrl()));
        doc.add(new TextField(IndexEntry.FILE_NAME, entry.getFileName(), Field.Store.YES));
        doc.add(new IntField(IndexEntry.CATEGORY, entry.getCategory().ordinal(), Field.Store.YES));
        doc.add(new TextField(IndexEntry.DESCRIPTION, entry.getDescription(), Field.Store.YES));
        doc.add(new StoredField(IndexEntry.HASH, entry.getHash()));
        if (entry.getLeaderId() == null)
            doc.add(new StringField(IndexEntry.LEADER_ID, new String(), Field.Store.YES));
        else
            doc.add(new StringField(IndexEntry.LEADER_ID, new BASE64Encoder().encode(entry.getLeaderId().getEncoded()), Field.Store.YES));

        if (entry.getFileSize() != 0) {
            doc.add(new LongField(IndexEntry.FILE_SIZE, entry.getFileSize(), Field.Store.YES));
        }

        if (entry.getUploaded() != null) {
            doc.add(new LongField(IndexEntry.UPLOADED, entry.getUploaded().getTime(),
                    Field.Store.YES));
        }

        if (entry.getLanguage() != null) {
            doc.add(new StringField(IndexEntry.LANGUAGE, entry.getLanguage(), Field.Store.YES));
        }
        adaptor.addDocumentToLucene(doc);
    }

    ;

    /**
     * Add a new {@link IndexEntry} to the local Lucene index.
     *
     * @param indexEntry the {@link IndexEntry} to be added
     * @throws IOException if the Lucene index fails to store the entry
     */
    private void addEntryLocal(IndexEntry indexEntry) throws IOException, LuceneAdaptorException {

        if (indexEntry.getId() < lowestMissingIndexValue
                || existingEntries.contains(indexEntry.getId())) {

            logger.warn("Trying to add duplicate IndexEntry at Node: " + self.getId() + " Index Entry Id: " + indexEntry.getId());
            return;
        }

        addIndexEntry(writeLuceneAdaptor, indexEntry);
        self.incrementNumberOfIndexEntries();

        // Inform other components about the IndexEntry Update.
        informListeningComponentsAboutUpdates(self);
//        Snapshot.incNumIndexEntries(self.getAddress());

        // Cancel gap detection timeouts for the given index
        UUID timeoutId = gapTimeouts.get(indexEntry.getId());
        if (timeoutId != null) {
            CancelTimeout ct = new CancelTimeout(timeoutId);
            trigger(ct, timerPort);
        }

        if (indexEntry.getId() == lowestMissingIndexValue) {
            // Search for the next missing index id
            do {
                existingEntries.remove(lowestMissingIndexValue);
                lowestMissingIndexValue++;
            } while (existingEntries.contains(lowestMissingIndexValue));
        } else if (indexEntry.getId() > lowestMissingIndexValue) {
            existingEntries.add(indexEntry.getId());
        }

        maxStoredId++;

        //update the counter, so we can check if partitioning is necessary
        if (leader && self.getPartitioningDepth() < config.getMaxPartitionIdLength())
            checkPartitioning();
    }

    private void checkPartitioning() {
        long numberOfEntries;

        /* Be careful of the mechanism in which the maxStoreId and minStoreId are updated. =========================== */
        numberOfEntries = Math.abs(maxStoredId - minStoredId);

        if (numberOfEntries < config.getMaxEntriesOnPeer())
            return;

        VodAddress.PartitioningType partitionsNumber = self.getPartitioningType();
        long medianId;

        if (maxStoredId > minStoredId) {
            medianId = (maxStoredId - minStoredId) / 2;
        } else {
            long values = numberOfEntries / 2;

            if (Long.MAX_VALUE - 1 - values > minStoredId)
                medianId = minStoredId + values;
            else {
                long thisPart = Long.MAX_VALUE - minStoredId - 1;
                values -= thisPart;
                medianId = Long.MIN_VALUE + values + 1;
            }
        }

        // Avoid start of partitioning in case if one is already going on.
        if (!partitionInProgress) {

            logger.warn(" Partitioning Message Initiated at : " + self.getId() + " with Minimum Id: " + minStoredId + " and MaxStoreId: " + maxStoredId);
            partitionInProgress = true;
            start2PhasePartitionCommit(minStoredId + medianId, partitionsNumber);
        }


    }

    /**
     * Starting point of the two phase commit protocol for partitioning commit in the
     * system.
     *
     * @param medianId         index entry split id.
     * @param partitioningType partitioning type
     */
    private void start2PhasePartitionCommit(long medianId, VodAddress.PartitioningType partitioningType) {

        if (leaderGroupInformation == null || leaderGroupInformation.size() < config.getLeaderGroupSize()) {
            logger.warn("Not enough nodes to start the two phase commit protocol.");
            return;
        }

        logger.debug("Going to start the two phase commit protocol.");
        partitionRequestId = java.util.UUID.randomUUID();

        PartitionHelper.PartitionInfo partitionInfo = new PartitionHelper.PartitionInfo(medianId, partitionRequestId, partitioningType);
        partitionInfo.setKey(publicKey);

        // Generate the hash information of the partition info for security purposes.
        String signedHash = ApplicationSecurity.generatePartitionInfoSignedHash(partitionInfo, privateKey);
        if (signedHash == null) {
            logger.error("Unable to generate a signed hash for the partitioning two phase commit.");
            throw new RuntimeException("Unable to generate hash for the partitioning two phase commit. ");
        }
        partitionInfo.setHash(signedHash);
        partitioningTracker.startTracking(partitionRequestId, leaderGroupInformation, partitionInfo);

        logger.warn(partitioningTracker.toString());

        // Create a timeout for the partition prepare response.
        se.sics.kompics.timer.ScheduleTimeout st = new se.sics.kompics.timer.ScheduleTimeout(config.getPartitionPrepareTimeout());
        PartitionPrepare.Timeout ppt = new PartitionPrepare.Timeout(st);
        st.setTimeoutEvent(ppt);
        
        partitionPreparePhaseTimeoutId = st.getTimeoutEvent().getTimeoutId();
        trigger(st, timerPort);

        
        PartitionPrepare.Request request = new PartitionPrepare.Request(partitionPreparePhaseTimeoutId, partitionInfo, new OverlayId(self.getOverlayId()));
        for (DecoratedAddress destination : leaderGroupInformation) {
            trigger(CommonHelper.getDecoratedContentMessage(self.getAddress(), destination, Transport.UDP, request), networkPort);
        }
    }


    /**
     * Partitioning Prepare Phase timed out, now resetting the partitioning information.
     * Be careful that the timeout can occur even if we have cancelled the timeout, this is the reason that we have to
     * externally track the timeout id to check if it has been reset by the application.
     * In case of sensitive timeouts, which can result in inconsistencies this step is necessary.
     */
    Handler<PartitionPrepare.Timeout> partitionPrepareTimeoutHandler = new Handler<PartitionPrepare.Timeout>() {
        @Override
        public void handle(PartitionPrepare.Timeout event) {

            if (partitionPreparePhaseTimeoutId == null || !partitionPreparePhaseTimeoutId.equals(event.getTimeoutId())) {

                logger.warn(" Partition Prepare Phase Timeout Occurred. ");
                partitionInProgress = false;
                partitioningTracker.resetTracker();

            }
        }
    };
    
    
    ClassMatchedHandler<PartitionPrepare.Request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, PartitionPrepare.Request>> handlerPartitionPrepareRequest = new ClassMatchedHandler<PartitionPrepare.Request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, PartitionPrepare.Request>>() {
        @Override
        public void handle(PartitionPrepare.Request request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, PartitionPrepare.Request> event) {
            
            logger.debug("{}: Received partition prepare request from : {}", self.getId(), event.getSource());
            // Step1: Verify that the data is from the leader only.
            if (!ApplicationSecurity.isPartitionUpdateValid(request.getPartitionInfo()) || !leaderIds.contains(request.getPartitionInfo().getKey())) {
                logger.error(" Partition Prepare Message Authentication Failed at: " + self.getId());
                return;
            }

            if (!partitionOrderValid(request.getOverlayId()))
                return;

            // Step2: Trigger the response for this request, which should be directly handled by the search component.
            PartitionPrepare.Response response = new PartitionPrepare.Response(request.getPartitionPrepareRoundId(), request.getPartitionInfo().getRequestId());
            trigger(CommonHelper.getDecoratedContentMessage(self.getAddress(), event.getSource(), Transport.UDP, response), networkPort);

            // Step3: Add timeout for this message.
            ScheduleTimeout st = new ScheduleTimeout(config.getPartitionCommitRequestTimeout());
            PartitionCommitTimeout pct = new PartitionCommitTimeout(st, request.getPartitionInfo());
            st.setTimeoutEvent(pct);

            // Step4: Add this to the map of pending partition updates.
            UUID timeoutId = st.getTimeoutEvent().getTimeoutId();
            PartitionHelper.PartitionInfo receivedPartitionInfo = request.getPartitionInfo();
            partitionUpdatePendingCommit.put(receivedPartitionInfo, timeoutId);
            trigger(st, timerPort);
            
        }
    };
    
    /**
     * This method basically prevents the nodes which rise quickly in the partition to avoid apply of updates, and apply the updates in order even though the update is being sent by the
     * leader itself. If not applied it screws up the min and max store id and lowestMissingIndexValues.
     * <p/>
     * DO NOT REMOVE THIS. (Prevents a rare fault case).
     *
     * @param overlayId OverlayId
     * @return applyPartitioningUpdate.
     */
    private boolean partitionOrderValid(OverlayId overlayId) {
        return (overlayId.getPartitioningType() == self.getPartitioningType() && overlayId.getPartitionIdDepth() == self.getPartitioningDepth());
    }


    Handler<PartitionCommitTimeout> handlePartitionCommitTimeout = new Handler<PartitionCommitTimeout>() {

        @Override
        public void handle(PartitionCommitTimeout event) {

            logger.warn("{}: Didn't receive any information regarding partition commit, so removing entry from the list.", self.getId());
            partitionUpdatePendingCommit.remove(event.getPartitionInfo());
        }
    };

    
    
    /**
     * Partition Prepare Response received from the leader group nodes. The leader once seeing the promises can either partition itself and then send the update to the nodes in the system
     * about the partitioning update or can first send the partitioning update to the leader group and then partition itself.
     * <p/>
     * CURRENTLY, the node sends the partitioning commit update to the nodes in the system and waits for the commit responses and then partition self, which might be wrong in our case.
     */

    ClassMatchedHandler<PartitionPrepare.Response, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, PartitionPrepare.Response>> handlerPartitionPrepareResponse  = new ClassMatchedHandler<PartitionPrepare.Response, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, PartitionPrepare.Response>>() {
        @Override
        public void handle(PartitionPrepare.Response response, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, PartitionPrepare.Response> event) {
            logger.debug("{}: Received response of the partition prepare message from: {}", self.getId(), event.getSource());

            partitioningTracker.addPromiseResponse(response);
            if (partitioningTracker.isPromiseAccepted()) {

                // Received the required responses. Start the commit phase.
                logger.warn("(PartitionPrepareMessage.Response): Time to start the commit phase. ");

                // Cancel the prepare phase timeout as all the replies have been received.
                se.sics.kompics.timer.CancelTimeout ct = new se.sics.kompics.timer.CancelTimeout(response.getPartitionPrepareRoundId());
                trigger(ct, timerPort);
                partitionPreparePhaseTimeoutId = null;

                // Create a commit timeout.
                ScheduleTimeout st = new ScheduleTimeout(config.getPartitionCommitTimeout());
                PartitionCommitMessage.Timeout pt = new PartitionCommitMessage.Timeout(st, partitioningTracker.getPartitionInfo());
                st.setTimeoutEvent(pt);
                partitionCommitPhaseTimeoutId = st.getTimeoutEvent().getTimeoutId();

                Collection<DecoratedAddress> leaderGroupAddress = partitioningTracker.getLeaderGroupNodes();
                PartitionCommit.Request request = new PartitionCommit.Request(partitionCommitPhaseTimeoutId, partitioningTracker.getPartitionRequestId());
                
                for (DecoratedAddress dest : leaderGroupAddress) {
                    trigger(CommonHelper.getDecoratedContentMessage(self.getAddress(), dest, Transport.UDP, request), networkPort);
                }
            }
            
        }
    };


    /**
     * Commit Phase Timeout Handler. At present we simply reset the partitioning tracker but do not address the issue that some nodes might have committed
     * the partitioning information and moved on.
     * <p/>
     * REQUIREMENT : Need a retry mechanism for the same, but not sure how to deal with inconsistent partitioning states in the system.
     */
    Handler<PartitionCommitMessage.Timeout> handlerPartitionCommitTimeoutMessage = new Handler<PartitionCommitMessage.Timeout>() {

        @Override
        public void handle(PartitionCommitMessage.Timeout event) {

            if (partitionCommitPhaseTimeoutId != null && partitionCommitPhaseTimeoutId.equals(event.getTimeoutId())) {

                logger.warn("Partition Commit Timeout Called at the leader.");
                partitionInProgress = false;
                partitioningTracker.resetTracker();
            }
        }
    };

    /**
     * Handler for the partition update commit.
     */

    ClassMatchedHandler<PartitionCommit.Request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, PartitionCommit.Request>> handlerPartitionCommitRequest = new ClassMatchedHandler<PartitionCommit.Request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, PartitionCommit.Request>>() {
        @Override
        public void handle(PartitionCommit.Request request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, PartitionCommit.Request> event) {

            logger.debug("{}: Received partition commit request from node: {}", self.getId(), event.getSource());

            UUID receivedPartitionRequestId = request.getPartitionRequestId();
            PartitionHelper.PartitionInfo partitionUpdate = null;

            for (PartitionHelper.PartitionInfo partitionInfo : partitionUpdatePendingCommit.keySet()) {

                if (partitionInfo.getRequestId().equals(receivedPartitionRequestId)) {
                    partitionUpdate = partitionInfo;
                    break;
                }
            }

            // No partition update entry present.
            if (partitionUpdate == null) {
                logger.warn(" Delayed Partition Message or False Partition Received by the Node.");
                return;
            }

            // If found, then cancel the timer.

            UUID cancelTimeoutId = partitionUpdatePendingCommit.get(partitionUpdate);
            CancelTimeout cancelTimeout = new CancelTimeout(cancelTimeoutId);
            trigger(cancelTimeout, timerPort);

            LinkedList<PartitionHelper.PartitionInfo> partitionUpdates = new LinkedList<PartitionHelper.PartitionInfo>();
            partitionUpdates.add(partitionUpdate);

            // Apply the partition update.
            applyPartitioningUpdate(partitionUpdates);
            partitionUpdatePendingCommit.remove(partitionUpdate);               // Remove the partition update from the pending map.

            // Send a  conformation to the leader.
            PartitionCommit.Response response = new PartitionCommit.Response(request.getPartitionCommitTimeout(), request.getPartitionRequestId());
            trigger(CommonHelper.getDecoratedContentMessage(self.getAddress(), event.getSource(), Transport.UDP, response), networkPort);
        }
    };



    /**
     * The partitioning commit response handler for the final phase of the two phase commit
     * regarding the partitioning commit.
     */
    ClassMatchedHandler<PartitionCommit.Response, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, PartitionCommit.Response>> handlerPartitionCommitResponse = new ClassMatchedHandler<PartitionCommit.Response, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, PartitionCommit.Response>>() {
        @Override
        public void handle(PartitionCommit.Response response, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, PartitionCommit.Response> event) {

            logger.trace("{}: Partitioning Commit Response received from: {}", self.getId(), event.getSource());
            partitioningTracker.addCommitResponse(response);
            if (partitioningTracker.isCommitAccepted()) {

                CancelTimeout ct = new CancelTimeout(response.getPartitionRequestId());
                trigger(ct, timerPort);
                partitionCommitPhaseTimeoutId = null;

                logger.debug("{}: Partitioning Protocol complete at the leader end.", self.getId());

                LinkedList<PartitionHelper.PartitionInfo> partitionUpdates = new LinkedList<PartitionHelper.PartitionInfo>();
                partitionUpdates.add(partitioningTracker.getPartitionInfo());

                applyPartitioningUpdate(partitionUpdates);
                partitioningTracker.resetTracker();
            }
        }
    };

    private IndexEntry createIndexEntryInternal(Document d, PublicKey pub) {
        IndexEntry indexEntry = new IndexEntry(d.get(IndexEntry.GLOBAL_ID),
                Long.valueOf(d.get(IndexEntry.ID)),
                d.get(IndexEntry.URL), d.get(IndexEntry.FILE_NAME),
                MsConfig.Categories.values()[Integer.valueOf(d.get(IndexEntry.CATEGORY))],
                d.get(IndexEntry.DESCRIPTION), d.get(IndexEntry.HASH), pub);

        String fileSize = d.get(IndexEntry.FILE_SIZE);
        if (fileSize != null)
            indexEntry.setFileSize(Long.valueOf(fileSize));

        String uploadedDate = d.get(IndexEntry.UPLOADED);
        if (uploadedDate != null)
            indexEntry.setUploaded(new Date(Long.valueOf(uploadedDate)));

        String language = d.get(IndexEntry.LANGUAGE);
        if (language != null)
            indexEntry.setLanguage(language);

        return indexEntry;
    }

    /**
     * Create an {@link IndexEntry} from the given document.
     *
     * @param d the document to create an {@link IndexEntry} from
     * @return an {@link IndexEntry} representing the given document
     */
    private IndexEntry createIndexEntry(Document d) {
        String leaderId = d.get(IndexEntry.LEADER_ID);

        if (leaderId == null)
            return createIndexEntryInternal(d, null);

        KeyFactory keyFactory;
        PublicKey pub = null;
        try {
            keyFactory = KeyFactory.getInstance("RSA");
            byte[] decode = Base64.decodeBase64(leaderId.getBytes());
            X509EncodedKeySpec publicKeySpec = new X509EncodedKeySpec(decode);
            pub = keyFactory.generatePublic(publicKeySpec);
        } catch (NoSuchAlgorithmException e) {
            logger.error(self.getId() + " " + e.getMessage());
        } catch (InvalidKeySpecException e) {
            logger.error(self.getId() + " " + e.getMessage());
        }

        return createIndexEntryInternal(d, pub);
    }

    /**
     * Check if an entry with the given id exists in the local index store.
     *
     * @param id the id of the entry
     * @return true if an entry with the given id exists
     * @throws IOException if Lucene errors occur
     */
    private boolean entryExists(long id) throws IOException {
        IndexEntry indexEntry = findById(id);
        return indexEntry != null ? true : false;
    }

    /**
     * Find an entry for the given id in the local index store.
     *
     * @param id the id of the entry
     * @return the entry if found or null if non-existing
     * @throws IOException if Lucene errors occur
     */
    private IndexEntry findById(long id) throws IOException {
        List<IndexEntry> indexEntries = ApplicationLuceneQueries.findIdRange(writeLuceneAdaptor, id, id, 1);
        if (indexEntries.isEmpty()) {
            return null;
        }
        return indexEntries.get(0);
    }





    /**
     * In case the dynamic utilities, if a leader partition, then it might happen that its utility falls below the nodes which have not yet partitioned.
     * Thus, it starts asking from the nodes which have not yet partitioned for the updates.
     *
     * @return true in case the message from node ahead in terms of partitioning.
     *
     * FIXME: Find a better solution of blocking index pull from higher nodes when partitioning happens.
     */
    /*
    private boolean isMessageFromNodeLaggingBehind(VodAddress address) {

        boolean result = false;

        OverlayAddress receivedOverlayAddress = new OverlayAddress(address);
        OverlayAddress selfOverlayAddress = new OverlayAddress(self.getAddress());

        if (!receivedOverlayAddress.getOverlayId().equals(selfOverlayAddress.getOverlayId())) {

            if (selfOverlayAddress.getPartitioningType() != VodAddress.PartitioningType.NEVER_BEFORE) {
                if ((selfOverlayAddress.getPartitionIdDepth() - receivedOverlayAddress.getPartitionIdDepth()) > 0) {
                    result = true;
                }
            }
        }
        return result;
    }
    */

    /**
     * Apply the partitioning updates received.
     */
    public void applyPartitioningUpdate(LinkedList<PartitionHelper.PartitionInfo> partitionUpdates) {

        for (PartitionHelper.PartitionInfo update : partitionUpdates) {

            boolean duplicateFound = false;
            for (PartitionHelper.PartitionInfo partitionInfo : partitionHistory) {
                if (partitionInfo.getRequestId().equals(update.getRequestId())) {
                    duplicateFound = true;
                    break;
                }
            }

            if (duplicateFound)
                continue;

            if (partitionHistory.size() >= HISTORY_LENGTH) {
                partitionHistory.removeFirst();
            }

            // Store the update in the history.
            partitionHistory.addLast(update);

            // Now apply the update.
            // Leader boolean simply sends true down the message in case of leader node, as it was implemented like this way before, not sure why.
            boolean partition = determineYourPartitionAndUpdatePartitionsNumberUpdated(update.getPartitioningTypeInfo());
            removeEntriesNotFromYourPartition(update.getMedianId(), partition);

            // Inform other components about the update.
            informListeningComponentsAboutUpdates(self);

        }
    }

    /**
     * Based on the source address, provide the control message enum that needs to be associated with the control response object.
     */
    private ControlMessageEnum getParitionUpdateStatus(OverlayId overlayId, List<PartitionHelper.PartitionInfoHash> partitionUpdateHashes) {

        boolean isOnePartition = self.getPartitioningType() == VodAddress.PartitioningType.ONCE_BEFORE;

        // for ONE_BEFORE
        if (isOnePartition) {
            if (overlayId.getPartitioningType() == VodAddress.PartitioningType.NEVER_BEFORE) {
                for (PartitionHelper.PartitionInfo partitionInfo : partitionHistory)
                    partitionUpdateHashes.add(new PartitionHelper.PartitionInfoHash(partitionInfo));
            }
        }

        // for MANY_BEFORE.
        else {

            int myDepth = self.getPartitioningDepth();
            if (overlayId.getPartitioningType() == VodAddress.PartitioningType.NEVER_BEFORE) {

                if (myDepth <= (HISTORY_LENGTH)) {
                    for (PartitionHelper.PartitionInfo partitionInfo : partitionHistory)
                        partitionUpdateHashes.add(new PartitionHelper.PartitionInfoHash(partitionInfo));
                } else
                    return ControlMessageEnum.REJOIN;
            } else {

                int receivedNodeDepth = overlayId.getPartitionIdDepth();
                if (myDepth - receivedNodeDepth > HISTORY_LENGTH)
                    return ControlMessageEnum.REJOIN;

                else if ((myDepth - receivedNodeDepth) <= (HISTORY_LENGTH) && (myDepth - receivedNodeDepth) > 0) {

                    // TODO : Test this condition.
                    int j = partitionHistory.size() - (myDepth - receivedNodeDepth);
                    for (int i = 0; i < (myDepth - receivedNodeDepth) && j < HISTORY_LENGTH; i++) {
                        partitionUpdateHashes.add(new PartitionHelper.PartitionInfoHash(partitionHistory.get(j)));
                        j++;
                    }
                }
            }
        }
        return ControlMessageEnum.PARTITION_UPDATE;
    }

    private boolean determineYourPartitionAndUpdatePartitionsNumberUpdated(VodAddress.PartitioningType partitionsNumber) {
        int nodeId = self.getId();

        PartitionId selfPartitionId = new PartitionId(partitionsNumber, self.getPartitioningDepth(),
                self.getPartitionId());

        boolean partitionSubId = PartitionHelper.determineYourNewPartitionSubId(nodeId, selfPartitionId);

        if (partitionsNumber == VodAddress.PartitioningType.NEVER_BEFORE) {
            int partitionId = (partitionSubId ? 1 : 0);

            int selfCategory = self.getCategoryId();
            int newOverlayId = OverlayIdHelper.encodePartitionDataAndCategoryIdAsInt(VodAddress.PartitioningType.ONCE_BEFORE,
                    1, partitionId, selfCategory);

            // CAUTION: Do not remove the below check.  Hell will break loose ...
            self.setOverlayId(newOverlayId);

        } else {
            int newPartitionId = self.getPartitionId() | ((partitionSubId ? 1 : 0) << self.getPartitionId());
            int selfCategory = self.getCategoryId();

            // Incrementing partitioning depth in the overlayId.
            int newOverlayId = OverlayIdHelper.encodePartitionDataAndCategoryIdAsInt(VodAddress.PartitioningType.MANY_BEFORE,
                    self.getPartitioningDepth() + 1, newPartitionId, selfCategory);
            self.setOverlayId(newOverlayId);
        }
        logger.debug("Partitioning Occurred at Node: " + self.getId() + " PartitionDepth: " + self.getPartitioningDepth() + " PartitionId: " + self.getPartitionId() + " PartitionType: " + self.getPartitioningType());
        
        return partitionSubId;
    }

    /**
     * Based on the unique ids return the partition updates back.
     *
     * @param partitionUpdatesIds
     * @return
     */
    public LinkedList<PartitionHelper.PartitionInfo> fetchPartitioningUpdates(List<java.util.UUID> partitionUpdatesIds) {

        LinkedList<PartitionHelper.PartitionInfo> partitionUpdates = new LinkedList<PartitionHelper.PartitionInfo>();
        for (java.util.UUID partitionUpdateId : partitionUpdatesIds) {

            boolean found = false;
            for (PartitionHelper.PartitionInfo partitionInfo : partitionHistory) {
                if (partitionInfo.getRequestId().equals(partitionUpdateId)) {
                    partitionUpdates.add(partitionInfo);
                    found = true;
                    break;
                }
            }

            if (!found) {
                break;
            }
        }
        return partitionUpdates;
    }

    /**
     * Request To check if the source address is behind in terms of partitioning updates.
     */
    private CheckPartitionInfoHashUpdate.Response getPartitionInfoHashUpdate(ControlInformation.Request event, DecoratedAddress source) {

        logger.debug("{}: Request for partition info received.", self.getId());
        
        LinkedList<PartitionHelper.PartitionInfoHash> partitionUpdateHashes = new LinkedList<PartitionHelper.PartitionInfoHash>();
        ControlMessageEnum controlMessageEnum = ControlMessageEnum.PARTITION_UPDATE;

        if (self.getPartitioningType() != VodAddress.PartitioningType.NEVER_BEFORE) {
            controlMessageEnum = getParitionUpdateStatus(event.getOverlayId(), partitionUpdateHashes);
        }
        
        return new CheckPartitionInfoHashUpdate.Response(event.getRequestId(), source, partitionUpdateHashes, controlMessageEnum);
    }


    /**
     * Push updated information to the listening components.
     *
     * @param self Updated Self
     */
    private void informListeningComponentsAboutUpdates(ApplicationSelf self) {

        SearchDescriptor updatedDesc = self.getSelfDescriptor();

        trigger(new SelfChangedPort.SelfChangedEvent(self), selfChangedPort);
        trigger(new CroupierUpdate<SearchDescriptor>(updatedDesc), croupierPortPositive);
        trigger(new SearchComponentUpdateEvent(new SearchComponentUpdate(updatedDesc, defaultComponentOverlayId)), statusAggregatorPortPositive);
        trigger(new ElectionLeaderUpdateEvent(new ElectionLeaderComponentUpdate(leader, defaultComponentOverlayId)), statusAggregatorPortPositive);
        trigger(new GradientUpdate<SearchDescriptor>(updatedDesc), gradientPort);
        trigger(new ViewUpdate(electionRound, updatedDesc), electionPort);
    }

    // ======= LEADER ELECTION PROTOCOL HANDLERS.

    /**
     * Node is elected as the leader of the partition.
     * In addition to this, node has chosen a leader group which it will work with.
     */
    Handler<LeaderState.ElectedAsLeader> leaderElectionHandler = new Handler<LeaderState.ElectedAsLeader>() {
        @Override
        public void handle(LeaderState.ElectedAsLeader event) {
            logger.warn("{}: Self node is elected as leader.", self.getId());
            leader = true;
            leaderGroupInformation = event.leaderGroup;

            BasicAddress selfPeerAddress = self.getAddress().getBase();
            Iterator<DecoratedAddress> itr = leaderGroupInformation.iterator();
            while (itr.hasNext()) {
                if (selfPeerAddress.equals(itr.next().getBase())) {
                    itr.remove();
                    break;
                }
            }

            informListeningComponentsAboutUpdates(self);
        }
    };

    /**
     * Node was the leader but due to a better node arriving in the system,
     * the leader gives up the leadership in order to maintain fairness.
     */
    Handler<LeaderState.TerminateBeingLeader> terminateBeingLeaderHandler = new Handler<LeaderState.TerminateBeingLeader>() {
        @Override
        public void handle(LeaderState.TerminateBeingLeader event) {
            logger.debug("{}: Self is being removed from the leadership position.", self.getId());
            leader = false;
            informListeningComponentsAboutUpdates(self);
        }
    };

    /**
     * Update about the current leader in the system.
     */
    Handler<LeaderUpdate> leaderUpdateHandler = new Handler<LeaderUpdate>() {
        @Override
        public void handle(LeaderUpdate event) {
            logger.debug("{}: Update regarding the leader in the system is received", self.getId());
            updateLeaderIds(event.leaderPublicKey);
        }
    };

    /**
     * Node is chosen by the leader to be part of a leader group. The utility of the node
     * should increase because of this.
     */
    Handler<ElectionState.EnableLGMembership> enableLGMembershipHandler = new Handler<ElectionState.EnableLGMembership>() {
        @Override
        public void handle(ElectionState.EnableLGMembership event) {

            logger.warn("{}: Node is chosen to be a part of leader group.", self.getId());
            self.setIsLGMember(true);
            electionRound = event.electionRoundId;
            informListeningComponentsAboutUpdates(self);
        }
    };

    /**
     * Node is no longer a part of leader group and therefore would not receive the entry addition directly
     * from the leader but they would have to pull it from the other neighbouring nodes in the system.
     */
    Handler<ElectionState.DisableLGMembership> disableLGMembershipHandler = new Handler<ElectionState.DisableLGMembership>() {
        @Override
        public void handle(ElectionState.DisableLGMembership event) {

            logger.warn("{}: Remove the node from the leader group membership.", self.getId());
            self.setIsLGMember(false);
            electionRound = event.electionRoundId;
            informListeningComponentsAboutUpdates(self);
        }
    };

}

