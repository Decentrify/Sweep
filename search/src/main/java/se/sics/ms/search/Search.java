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
import se.sics.cm.ports.ChunkManagerPort;
import se.sics.co.FailureDetectorPort;
import se.sics.gvod.common.msgs.MessageDecodingException;
import se.sics.gvod.common.msgs.MessageEncodingException;
import se.sics.gvod.config.SearchConfiguration;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.VodNetwork;
import se.sics.gvod.timer.*;
import se.sics.gvod.timer.Timer;
import se.sics.gvod.timer.UUID;
import se.sics.kompics.*;
import se.sics.ms.aggregator.SearchComponentUpdate;
import se.sics.ms.aggregator.SearchComponentUpdateEvent;
import se.sics.ms.aggregator.port.StatusAggregatorPort;
import se.sics.ms.common.*;
import se.sics.ms.configuration.MsConfig;
import se.sics.ms.control.*;
import se.sics.ms.events.UiAddIndexEntryRequest;
import se.sics.ms.events.UiAddIndexEntryResponse;
import se.sics.ms.events.UiSearchRequest;
import se.sics.ms.events.UiSearchResponse;
import se.sics.ms.exceptions.IllegalSearchString;
import se.sics.ms.gradient.control.*;
import se.sics.ms.gradient.events.*;
import se.sics.ms.gradient.ports.GradientRoutingPort;
import se.sics.ms.gradient.ports.LeaderStatusPort;
import se.sics.ms.gradient.ports.PublicKeyPort;
import se.sics.ms.messages.*;
import se.sics.ms.model.LocalSearchRequest;
import se.sics.ms.model.PartitionReplicationCount;
import se.sics.ms.model.PeerControlMessageRequestHolder;
import se.sics.ms.model.ReplicationCount;
import se.sics.ms.ports.SelfChangedPort;
import se.sics.ms.ports.SimulationEventsPort;
import se.sics.ms.ports.SimulationEventsPort.AddIndexSimulated;
import se.sics.ms.ports.UiPort;
import se.sics.ms.snapshot.Snapshot;
import se.sics.ms.timeout.AwaitingForCommitTimeout;
import se.sics.ms.timeout.CommitTimeout;
import se.sics.ms.timeout.IndividualTimeout;
import se.sics.ms.timeout.PartitionCommitTimeout;
import se.sics.ms.types.*;
import se.sics.ms.types.OverlayId;
import se.sics.ms.util.Pair;
import se.sics.ms.util.PartitionHelper;
import se.sics.p2ptoolbox.croupier.api.CroupierPort;
import se.sics.p2ptoolbox.croupier.api.msg.CroupierUpdate;
import se.sics.p2ptoolbox.gradient.api.GradientPort;
import se.sics.p2ptoolbox.gradient.api.msg.GradientSample;
import se.sics.p2ptoolbox.gradient.api.msg.GradientUpdate;
import sun.misc.BASE64Encoder;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.security.*;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.util.*;
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

    Positive<SimulationEventsPort> simulationEventsPort = positive(SimulationEventsPort.class);
    Positive<VodNetwork> networkPort = positive(VodNetwork.class);
    Positive<ChunkManagerPort> chunkManagerPort = positive(ChunkManagerPort.class);
    Positive<Timer> timerPort = positive(Timer.class);
    Positive<GradientRoutingPort> gradientRoutingPort = positive(GradientRoutingPort.class);
    Positive<FailureDetectorPort> fdPort = requires(FailureDetectorPort.class);
    Negative<LeaderStatusPort> leaderStatusPort = negative(LeaderStatusPort.class);
    Negative<PublicKeyPort> publicKeyPort = negative(PublicKeyPort.class);
    Negative<UiPort> uiPort = negative(UiPort.class);
    Negative<SelfChangedPort> selfChangedPort = negative(SelfChangedPort.class);
    Positive<CroupierPort> croupierPortPositive = requires(CroupierPort.class);
    Positive<StatusAggregatorPort> statusAggregatorPortPositive = requires(StatusAggregatorPort.class);
    Positive<GradientPort> gradientPort = requires(GradientPort.class);

    private static final Logger logger = LoggerFactory.getLogger(Search.class);
    private MsSelfImpl self;
    private SearchConfiguration config;
    private boolean leader;
    // The last lowest missing index value
    private long lowestMissingIndexValue;
    // Set of existing entries higher than the lowestMissingIndexValue
    private SortedSet<Long> existingEntries;
    // The last id used for adding new entries in case this node is the leader
    private long nextInsertionId;
    // Data structure to keep track of acknowledgments for newly added indexes
    private Map<TimeoutId, ReplicationCount> replicationRequests;
    private Map<TimeoutId, ReplicationCount> commitRequests;
    // Structure that maps index ids to UUIDs of open gap timeouts
    private Map<Long, UUID> gapTimeouts;
    // Set of recent add requests to avoid duplication
    private Map<TimeoutId, Long> recentRequests;

    // Apache Lucene used for searching
    private StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_42);
    private Directory index;
    private IndexWriterConfig indexWriterConfig = new IndexWriterConfig(Version.LUCENE_42, analyzer);

    // Lucene variables used to store and search in collected answers
    private LocalSearchRequest searchRequest;
    private Directory searchIndex;

    // Aggregator Variable.
    private int defaultComponentOverlayId = 0;
    
    private PrivateKey privateKey;
    private PublicKey publicKey;
    private ArrayList<PublicKey> leaderIds = new ArrayList<PublicKey>();
    //private HashMap<TimeoutId, IndexEntry> awaitingForPrepairResponse = new HashMap<TimeoutId, IndexEntry>();
    private HashMap<IndexEntry, TimeoutId> pendingForCommit = new HashMap<IndexEntry, TimeoutId>();
    private HashMap<TimeoutId, TimeoutId> replicationTimeoutToAdd = new HashMap<TimeoutId, TimeoutId>();
    private HashMap<TimeoutId, Integer> searchPartitionsNumber = new HashMap<TimeoutId, Integer>();

    private HashMap<PartitionHelper.PartitionInfo, TimeoutId> partitionUpdatePendingCommit = new HashMap<PartitionHelper.PartitionInfo, TimeoutId>();
    private long minStoredId = Long.MIN_VALUE;
    private long maxStoredId = Long.MIN_VALUE;

    private HashMap<TimeoutId, Long> timeStoringMap = new HashMap<TimeoutId, Long>();
    private static HashMap<TimeoutId, Pair<Long, Integer>> searchRequestStarted = new HashMap<TimeoutId, Pair<Long, Integer>>();
    private TimeoutId partitionRequestId;
    private boolean partitionInProgress = false;
    private Map<TimeoutId, PartitionReplicationCount> partitionPrepareReplicationCountMap = new HashMap<TimeoutId, PartitionReplicationCount>();
    private Map<TimeoutId, PartitionReplicationCount> partitionCommitReplicationCountMap = new HashMap<TimeoutId, PartitionReplicationCount>();

    private TimeoutId controlMessageExchangeRoundId;
    private Map<VodAddress, TimeoutId> peerControlMessageAddressRequestIdMap = new HashMap<VodAddress, TimeoutId>();
    private Map<VodAddress, PeerControlMessageRequestHolder> peerControlMessageResponseMap = new HashMap<VodAddress, PeerControlMessageRequestHolder>();
    private Map<ControlMessageResponseTypeEnum, List<? extends ControlBase>> controlMessageResponseHolderMap = new HashMap<ControlMessageResponseTypeEnum, List<? extends ControlBase>>();
    private int controlMessageResponseCount = 0;
    private boolean partitionUpdateFetchInProgress = false;
    private TimeoutId currentPartitionInfoFetchRound;

    private LinkedList<PartitionHelper.PartitionInfo> partitionHistory;
    private static final int HISTORY_LENGTH = 5;
    private LuceneAdaptor writeLuceneAdaptor;
    private LuceneAdaptor searchRequestLuceneAdaptor;

    private class ExchangeRound extends IndividualTimeout {

        public ExchangeRound(SchedulePeriodicTimeout request, int id) {
            super(request, id);
        }
    }

    // Control Message Exchange Round.
    private class ControlMessageExchangeRound extends IndividualTimeout {

        public ControlMessageExchangeRound(SchedulePeriodicTimeout request, int id) {
            super(request, id);
        }
    }

    private class SearchTimeout extends IndividualTimeout {

        public SearchTimeout(ScheduleTimeout request, int id) {
            super(request, id);
        }
    }

    private class IndexExchangeTimeout extends IndividualTimeout {

        public IndexExchangeTimeout(ScheduleTimeout request, int id) {
            super(request, id);
        }
    }

    /**
     * Periodic scheduled timeout event to garbage collect the recent request
     * data structure of {@link Search}.
     */
    private class RecentRequestsGcTimeout extends IndividualTimeout {

        public RecentRequestsGcTimeout(SchedulePeriodicTimeout request, int id) {
            super(request, id);
        }
    }

    /**
     * Timeout for waiting for an {@link se.sics.ms.messages.AddIndexEntryMessage.Response} acknowledgment for an
     * {@link se.sics.ms.messages.AddIndexEntryMessage.Response} request.
     */
    private static class AddIndexTimeout extends IndividualTimeout {
        private final int retryLimit;
        private int numberOfRetries = 0;
        private final IndexEntry entry;

        /**
         * @param request    the ScheduleTimeout that holds the Timeout
         * @param retryLimit the number of retries for the related
         *                   {@link se.sics.ms.messages.AddIndexEntryMessage.Request}
         * @param entry      the {@link se.sics.ms.types.IndexEntry} this timeout was scheduled for
         */
        public AddIndexTimeout(ScheduleTimeout request, int id, int retryLimit, IndexEntry entry) {
            super(request, id);
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
        subscribe(handleGradientIndexHashExchangeResponse, gradientRoutingPort);
        subscribe(handleIndexHashExchangeResponse, networkPort);
        subscribe(handleIndexHashExchangeResponse, chunkManagerPort);
        subscribe(handleIndexExchangeRequest, networkPort);
        subscribe(handleIndexExchangeResponse, networkPort);
        subscribe(handleIndexExchangeResponse, chunkManagerPort);
        subscribe(handleAddIndexEntryRequest, networkPort);
        subscribe(handleAddIndexEntryResponse, networkPort);
        subscribe(handleSearchRequest, networkPort);
        subscribe(handleSearchResponse, chunkManagerPort);
        subscribe(handleSearchResponse, networkPort);
        subscribe(handleSearchTimeout, timerPort);
        subscribe(handleAddRequestTimeout, timerPort);
        subscribe(handleRecentRequestsGcTimeout, timerPort);
        subscribe(handleLeaderStatus, leaderStatusPort);
        subscribe(handleLeaderUpdate, leaderStatusPort);
        subscribe(searchRequestHandler, uiPort);
        subscribe(handleRepairRequest, networkPort);
        subscribe(handleRepairResponse, networkPort);
        subscribe(handlePrepareCommit, networkPort);
        subscribe(handleAwaitingForCommitTimeout, timerPort);
        subscribe(handlePrepareCommitResponse, networkPort);
        subscribe(handleCommitTimeout, timerPort);
        subscribe(handleCommitRequest, networkPort);
        subscribe(handleCommitResponse, networkPort);
        subscribe(addIndexEntryRequestHandler, uiPort);
        subscribe(handleSearchSimulated, simulationEventsPort);
        subscribe(handleViewSizeResponse, gradientRoutingPort);
        subscribe(handleIndexExchangeTimeout, timerPort);
        subscribe(handleNumberOfPartitions, gradientRoutingPort);
        // Two Phase Commit Mechanism.
        subscribe(partitionPrepareTimeoutHandler, timerPort);
        subscribe(handlerPartitionPrepareRequest, networkPort);
        subscribe(handlerPartitionPrepareResponse, networkPort);
        subscribe(handlePartitionCommitTimeout, timerPort);
        subscribe(handlerPartitionCommitRequest, networkPort);
        subscribe(handlerPartitionCommitResponse, networkPort);
        subscribe(handlerLeaderGroupInformationResponse, gradientRoutingPort);
        subscribe(handlerPartitionCommitTimeoutMessage, timerPort);
        // Generic Control message exchange mechanism
        subscribe(handlerControlMessageExchangeRound, timerPort);
        subscribe(handlerControlMessageRequest, networkPort);
        subscribe(handlerControlMessageInternalResponse, gradientRoutingPort);
        subscribe(handlerControlMessageResponse, networkPort);
        subscribe(handlerDelayedPartitioningMessageRequest, networkPort);
        subscribe(delayedPartitioningTimeoutHandler, timerPort);
        subscribe(delayedPartitioningResponseHandler, networkPort);
        subscribe(gradientSampleHandler, gradientPort);
    }

    /**
     * Initialize the component.
     */
    private void doInit(SearchInit init) {

        self = (MsSelfImpl) init.getSelf();

        config = init.getConfiguration();
        KeyPairGenerator keyGen;
        try {
            keyGen = KeyPairGenerator.getInstance("RSA");
            keyGen.initialize(1024);
            final KeyPair key = keyGen.generateKeyPair();
            privateKey = key.getPrivate();
            publicKey = key.getPublic();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        replicationRequests = new HashMap<TimeoutId, ReplicationCount>();
        nextInsertionId = 0;
        lowestMissingIndexValue = 0;
        commitRequests = new HashMap<TimeoutId, ReplicationCount>();
        existingEntries = new TreeSet<Long>();
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
//            logger.warn(" Writer Lucene Adapter Instantiated. ");
            writeLuceneAdaptor.initialEmptyWriterCommit();
            
            if (PERSISTENT_INDEX && file != null && file.exists()) {
                initializeIndexCaches(writeLuceneAdaptor);
            }
            
            minStoredId = getMinStoredIdFromLucene(writeLuceneAdaptor);
            maxStoredId = getMaxStoredIdFromLucene(writeLuceneAdaptor);

        } catch (LuceneAdaptorException e) {
            // Proper exception handling.
            e.printStackTrace();
            System.exit(-1);
        }
        recentRequests = new HashMap<TimeoutId, Long>();

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

            informListeningComponentsAboutUpdates(self);
            SchedulePeriodicTimeout rst = new SchedulePeriodicTimeout(
                    config.getRecentRequestsGcInterval(),
                    config.getRecentRequestsGcInterval());
            rst.setTimeoutEvent(new RecentRequestsGcTimeout(rst, self.getId()));
            trigger(rst, timerPort);

            // TODO move time to own config instead of using the gradient period. IndexHashExchangePeriod.
            rst = new SchedulePeriodicTimeout(MsConfig.GRADIENT_SHUFFLE_PERIOD, MsConfig.GRADIENT_SHUFFLE_PERIOD);
            rst.setTimeoutEvent(new ExchangeRound(rst, self.getId()));
            trigger(rst, timerPort);

            // Bootup the croupier with default configuration.
            CroupierUpdate initialCroupierBootupUpdate = new CroupierUpdate(java.util.UUID.randomUUID(), new SearchDescriptor(new OverlayAddress(self.getAddress()), 0, false, 0, 0));
            trigger(initialCroupierBootupUpdate, croupierPortPositive);
            
            rst = new SchedulePeriodicTimeout(MsConfig.CONTROL_MESSAGE_EXCHANGE_PERIOD, MsConfig.CONTROL_MESSAGE_EXCHANGE_PERIOD);
            rst.setTimeoutEvent(new ControlMessageExchangeRound(rst, self.getId()));
            trigger(rst, timerPort);
        }
    };

    /**
     * Initialize the Index Caches, from the indexes stored in files.
     *
     * @param luceneAdaptor LuceneAdaptor for access to lucene instance.
     * @throws LuceneAdaptorException
     */
    public void initializeIndexCaches(LuceneAdaptor luceneAdaptor) throws LuceneAdaptorException{

        boolean continuous = true;
        int readLimit = 20000;

        for (long i = 0; ; i += readLimit) {
            Query query = NumericRangeQuery.newLongRange(IndexEntry.ID, i, i + readLimit, true, false);
            List<IndexEntry> indexEntryList =luceneAdaptor.searchIndexEntriesInLucene(query, new Sort(new SortField(IndexEntry.ID, Type.LONG)), readLimit);
            
            if(indexEntryList.isEmpty()){
                logger.info("Empty entry list retrieved from the index.");
                break;
            }

            if (continuous) {
                Long[] ids = new Long[indexEntryList.size()];
                
                for(int j =0, x = indexEntryList.size(); j < x ; j ++){
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
            }
            
            else{
                for(int j =0, x = indexEntryList.size(); j < x ; j ++){
                    existingEntries.add(indexEntryList.get(j).getId());
                }
            }
        }
    }

    private boolean exchangeInProgress = false;
    private TimeoutId indexExchangeTimeout;
    private HashSet<VodAddress> nodesSelectedForIndexHashExchange = new HashSet<VodAddress>();
    private HashSet<VodAddress> nodesRespondedInIndexHashExchange = new HashSet<VodAddress>();
    private HashMap<VodAddress, Collection<IndexHash>> collectedHashes = new HashMap<VodAddress, Collection<IndexHash>>();
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
    Handler<ControlMessageExchangeRound> handlerControlMessageExchangeRound = new Handler<ControlMessageExchangeRound>() {
        @Override
        public void handle(ControlMessageExchangeRound event) {

            logger.debug("Initiated the Periodic Exchange Timeout.");

            //Clear the previous rounds data to avoid clash in the responses.
            cleanControlMessageResponseData();

            //Trigger the new exchange round.
            controlMessageExchangeRoundId = UUID.nextUUID();
            trigger(new GradientRoutingPort.InitiateControlMessageExchangeRound(controlMessageExchangeRoundId, config.getIndexExchangeRequestNumber()), gradientRoutingPort);
        }
    };


    /**
     * Control Message Request Received.
     */
    Handler<ControlMessage.Request> handlerControlMessageRequest = new Handler<ControlMessage.Request>() {
        @Override
        public void handle(ControlMessage.Request event) {

            logger.debug(" Received the Control Message Request at: " + self.getId());

            // Check if already entry present and reset the contents.
            if (peerControlMessageResponseMap.get(event.getVodSource().getId()) != null)
                peerControlMessageResponseMap.get(event.getVodSource().getId()).reset();

                // Else create a fresh entry, with number of responses to keep track of.
            else
                peerControlMessageResponseMap.put(event.getVodSource(), new PeerControlMessageRequestHolder(config.getControlMessageEnumSize()));

            // Update the latest request id from the peer control message from the map.
            peerControlMessageAddressRequestIdMap.put(event.getVodSource(), event.getRoundId());

            // Information amount partition is available in Search, just fetch locally
            handleControlMessageInternalResponseEvent(getCheckPartitionInfoHashUpdateResponse(event));
            // Request info from the gradient component
            trigger(new CheckLeaderInfoUpdate.Request(event.getRoundId(), event.getVodSource()), gradientRoutingPort);
        }
    };

    void handleControlMessageInternalResponseEvent(ControlMessageInternal.Response event) {

        try {

            TimeoutId roundIdReceived = event.getRoundId();
            TimeoutId currentRoundId = peerControlMessageAddressRequestIdMap.get(event.getSourceAddress());

            // Perform initial checks to avoid old responses.
            if (currentRoundId == null || !currentRoundId.equals(roundIdReceived)) {
                return;
            }

            // Update the peer control response map to add the new entry.
            PeerControlMessageRequestHolder controlMessageResponse = peerControlMessageResponseMap.get(event.getSourceAddress());
            if (controlMessageResponse == null) {
                logger.error(" Not able to Locate Response Object for Node: " + event.getSourceAddress().getId());
                return;
            }

            ByteBuf buf = controlMessageResponse.getBuffer();
            // Fetch the buffer to write and append the information in it.
            ControlMessageEncoderFactory.encodeControlMessageInternal(buf, event);

            // encansuplate it into a separate method.
            if (controlMessageResponse.addAndCheckStatus()) {

                // Construct the response object and trigger it back to the user.
                logger.debug(" Ready To Send back Control Message Response to the Requestor. ");

                // Send the data back to the user.
                trigger(new ControlMessage.Response(self.getAddress(), event.getSourceAddress(), event.getRoundId(), buf.array()), networkPort);

                // TODO: Some kind of timeout mechanism needs to be there because there might be some issue with the components and hence they don't reply on time.

                // Clean the data at the user end also.
                cleanControlRequestMessageData(event.getSourceAddress());
            }
        } catch (MessageEncodingException e) {
            // If the exception is thrown it means that encoding was not successful for some reason and we will return after raising a flag ...
            logger.error(" Encoding Failed ... Please Check ... ");
            e.printStackTrace();
            // Clean the data at the user end also.
            cleanControlRequestMessageData(event.getSourceAddress());
        }
    }


    /**
     * Received the partitioning updates from the gradient component.
     */
    Handler<ControlMessageInternal.Response> handlerControlMessageInternalResponse = new Handler<ControlMessageInternal.Response>() {

        @Override
        public void handle(ControlMessageInternal.Response event) {

            handleControlMessageInternalResponseEvent(event);
        }
    };


    /**
     * Simply remove the data in the maps belonging to the address id for the
     *
     * @param sourceAddress
     */
    public void cleanControlRequestMessageData(VodAddress sourceAddress) {

        logger.debug(" Clean Control Message Data Called at : " + self.getId());
        peerControlMessageAddressRequestIdMap.remove(sourceAddress);
        peerControlMessageResponseMap.remove(sourceAddress);

    }

    /**
     * Control Message Response containing important information.
     */
    Handler<ControlMessage.Response> handlerControlMessageResponse = new Handler<ControlMessage.Response>() {
        @Override
        public void handle(ControlMessage.Response event) {

            //Step1: Filter the old responses.
            if (!controlMessageExchangeRoundId.equals(event.getRoundId()))
                return;

            // Create an instance of ByteBuf to read the data received.
            ByteBuf buffer = Unpooled.wrappedBuffer(event.getBytes());

            try {

                int numOfIterations = ControlMessageDecoderFactory.getNumberOfUpdates(buffer);

                // Check if more control messages available in the buffer.
                while (numOfIterations > 0) {

                    ControlBase controlMessageInternalResponse = ControlMessageDecoderFactory.decodeControlMessageInternal(buffer, event);

                    if (controlMessageInternalResponse != null)
                        ControlMessageHelper.updateTheControlMessageResponseHolderMap(controlMessageInternalResponse, controlMessageResponseHolderMap);

                    // Handles iterations within a response.
                    numOfIterations -= 1;
                }

                // Handles multiple responses.
                controlMessageResponseCount++;

                if (controlMessageResponseCount >= config.getIndexExchangeRequestNumber()) {


                    // Perform the checks and comparison on the responses received from the nodes.
                    performControlMessageResponseMatching();

                    // Perform the cleaning of the data after this.
                    cleanControlMessageResponseData();
                }
            } catch (MessageDecodingException e) {
                logger.error(" Message Decodation Failed at :" + self.getAddress().getId());
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

        VodAddress newLeader = null;
        PublicKey newLeaderPublicKey = null;
        boolean isFirst = true;
        //agree to a leader only if all received responses have leader as null or
        // points to the same exact same leader.
        boolean hasAgreedLeader = true;

        for (LeaderInfoControlResponse leaderInfo : leaderControlResponses) {

            VodAddress currentLeader = leaderInfo.getLeaderAddress();
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

    Handler<LeaderInfoUpdate> handleLeaderUpdate = new Handler<LeaderInfoUpdate>() {
        @Override
        public void handle(LeaderInfoUpdate leaderInfoUpdate) {

            updateLeaderIds(leaderInfoUpdate.getLeaderPublicKey());
        }
    };


    /**
     * Extract the partition updates who's hashes match but sequence should not be violated.
     *
     * @param partitionControlResponses
     */
    private void performPartitionUpdateMatching(List<PartitionControlResponse> partitionControlResponses) {

        Iterator<PartitionControlResponse> iterator = partitionControlResponses.iterator();

        List<TimeoutId> finalPartitionUpdates = new ArrayList<TimeoutId>();
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
        Random random = new Random();
        // request for the updates from any random node.
        VodAddress randomPeerAddress = partitionControlResponses.get(random.nextInt(partitionControlResponses.size())).getSourceAddress();


        TimeoutId timeoutId = UUID.nextUUID();
        ScheduleTimeout st = new ScheduleTimeout(config.getDelayedPartitioningRequestTimeout());
        DelayedPartitioningMessage.Timeout delayedPartitioningTimeout = new DelayedPartitioningMessage.Timeout(st, self.getId());
        st.setTimeoutEvent(delayedPartitioningTimeout);
        st.getTimeoutEvent().setTimeoutId(timeoutId);

        currentPartitionInfoFetchRound = timeoutId;
        partitionUpdateFetchInProgress = true;

        // Trigger the new updates.
        trigger(new DelayedPartitioningMessage.Request(self.getAddress(), randomPeerAddress, timeoutId, finalPartitionUpdates), networkPort);

        // Trigger the Scehdule Timeout Event.
        trigger(st, timerPort);
    }


    /**
     * Received Request for the Partitioning Updates.
     */
    Handler<DelayedPartitioningMessage.Request> handlerDelayedPartitioningMessageRequest = new Handler<DelayedPartitioningMessage.Request>() {

        @Override
        public void handle(DelayedPartitioningMessage.Request event) {

            logger.debug(" Delayed Partitioning Message Request Received from: " + event.getSource().getId());
            LinkedList<PartitionHelper.PartitionInfo> partitionUpdates = fetchPartitioningUpdates(event.getPartitionRequestIds());

            trigger(new DelayedPartitioningMessage.Response(self.getAddress(), event.getVodSource(), event.getTimeoutId(), partitionUpdates), networkPort);

        }
    };

    /**
     * Apply the partitioning updates to the local.
     */
    Handler<DelayedPartitioningMessage.Response> delayedPartitioningResponseHandler = new Handler<DelayedPartitioningMessage.Response>() {

        @Override
        public void handle(DelayedPartitioningMessage.Response event) {

            if (!partitionUpdateFetchInProgress || !(event.getTimeoutId().equals(currentPartitionInfoFetchRound)))
                return;

            // Cancel the timeout event.
            CancelTimeout cancelTimeout = new CancelTimeout(event.getTimeoutId());
            trigger(cancelTimeout, timerPort);

            // Simply apply the partitioning update and handle the duplicacy.
            applyPartitioningUpdate(event.getPartitionHistory());
        }
    };

    /**
     * Handler for the Delayed Partitioning Timeout.
     */
    Handler<DelayedPartitioningMessage.Timeout> delayedPartitioningTimeoutHandler = new Handler<DelayedPartitioningMessage.Timeout>() {

        @Override
        public void handle(DelayedPartitioningMessage.Timeout event) {

            //Reset the partitionUpdateInProgress.
            // Can also set the current partitioning round to a no timeout object.
            partitionUpdateFetchInProgress = false;
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
    final Handler<ExchangeRound> handleRound = new Handler<ExchangeRound>() {
        @Override
        public void handle(ExchangeRound event) {
            if (exchangeInProgress) {
                return;
            }

            exchangeInProgress = true;

            ScheduleTimeout timeout = new ScheduleTimeout(config.getIndexExchangeTimeout());
            timeout.setTimeoutEvent(new IndexExchangeTimeout(timeout, self.getId()));

            indexExchangeTimeout = timeout.getTimeoutEvent().getTimeoutId();

            trigger(timeout, timerPort);
            collectedHashes.clear();
            nodesSelectedForIndexHashExchange.clear();
            nodesRespondedInIndexHashExchange.clear();

            Long[] existing = existingEntries.toArray(new Long[existingEntries.size()]);
            trigger(new GradientRoutingPort.IndexHashExchangeRequest(lowestMissingIndexValue, existing,
                    indexExchangeTimeout, config.getIndexExchangeRequestNumber()), gradientRoutingPort);
        }
    };

    /**
     * Search for entries in the local store that the inquirer might need and
     * send the ids and hashes.
     */
    final Handler<IndexHashExchangeMessage.Request> handleIndexHashExchangeRequest = new Handler<IndexHashExchangeMessage.Request>() {
        @Override
        public void handle(IndexHashExchangeMessage.Request event) {

            try {

                List<IndexHash> hashes = new ArrayList<IndexHash>();

                // Search for entries the inquirer is missing
                long lastId = event.getOldestMissingIndexValue();
                for (long i : event.getExistingEntries()) {
                    Collection<IndexEntry> indexEntries = findIdRange(writeLuceneAdaptor, lastId, i - 1, config.getMaxExchangeCount() - hashes.size());
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
                    Collection<IndexEntry> indexEntries = findIdRange(writeLuceneAdaptor,lastId, Long.MAX_VALUE, config.getMaxExchangeCount() - hashes.size());
                    for (IndexEntry indexEntry : indexEntries) {
                        hashes.add((new IndexHash(indexEntry)));
                    }
                }

                trigger(new IndexHashExchangeMessage.Response(self.getAddress(), event.getVodSource(), event.getTimeoutId(), hashes), chunkManagerPort);
            } catch (IOException e) {
                logger.error(self.getId() + " " + e.getMessage());
            }
        }
    };

    final Handler<GradientRoutingPort.IndexHashExchangeResponse> handleGradientIndexHashExchangeResponse = new Handler<GradientRoutingPort.IndexHashExchangeResponse>() {
        @Override
        public void handle(GradientRoutingPort.IndexHashExchangeResponse indexHashExchangeResponse) {

            nodesSelectedForIndexHashExchange = indexHashExchangeResponse.getNodesSelectedForExchange();
        }
    };

    final Handler<IndexHashExchangeMessage.Response> handleIndexHashExchangeResponse = new Handler<IndexHashExchangeMessage.Response>() {
        @Override
        public void handle(IndexHashExchangeMessage.Response event) {
            // Drop old responses
            if (!event.getTimeoutId().equals(indexExchangeTimeout)) {
                return;
            }

            nodesRespondedInIndexHashExchange.add(event.getVodSource());

            // TODO we somehow need to check here that the answer is from the correct node
            collectedHashes.put(event.getVodSource(), event.getHashes());
            if (collectedHashes.size() == config.getIndexExchangeRequestNumber()) {
                intersection = new HashSet<IndexHash>(collectedHashes.values().iterator().next());
                for (Collection<IndexHash> hashes : collectedHashes.values()) {
                    intersection.retainAll(hashes);
                }

                if (intersection.isEmpty()) {
                    CancelTimeout cancelTimeout = new CancelTimeout(indexExchangeTimeout);
                    trigger(cancelTimeout, timerPort);
                    indexExchangeTimeout = null;
                    exchangeInProgress = false;
                    return;
                }

                ArrayList<Id> ids = new ArrayList<Id>();
                for (IndexHash hash : intersection) {
                    ids.add(hash.getId());
                }

                VodAddress node = collectedHashes.keySet().iterator().next();
                trigger(new IndexExchangeMessage.Request(self.getAddress(), node, event.getTimeoutId(), ids), networkPort);
            }
        }
    };

    /**
     * Search for entries in the local store that the inquirer requested and
     * send them to him.
     */
    final Handler<IndexExchangeMessage.Request> handleIndexExchangeRequest = new Handler<IndexExchangeMessage.Request>() {
        @Override
        public void handle(IndexExchangeMessage.Request event) {
            try {
                List<IndexEntry> indexEntries = new ArrayList<IndexEntry>();
                for (Id id : event.getIds()) {
                    // TODO use the leader id to search
                    IndexEntry entry = findById(id.getId());
                    if (entry != null)
                        indexEntries.add(entry);
                }

                trigger(new IndexExchangeMessage.Response(self.getAddress(), event.getVodSource(), event.getTimeoutId(), indexEntries, 0, 0), chunkManagerPort);
            } catch (IOException e) {
                logger.error(self.getId() + " " + e.getMessage());
            }
        }
    };

    /**
     * Add all entries received from another node to the local index store.
     */
    final Handler<IndexExchangeMessage.Response> handleIndexExchangeResponse = new Handler<IndexExchangeMessage.Response>() {
        @Override
        public void handle(IndexExchangeMessage.Response event) {
            // Drop old responses
            if (!event.getTimeoutId().equals(indexExchangeTimeout)) {
                return;
            }

            // Stop accepting responses from lagging behind nodes.
            if (isMessageFromNodeLaggingBehind(event.getVodSource())) {
                return;
            }


            CancelTimeout cancelTimeout = new CancelTimeout(indexExchangeTimeout);
            trigger(cancelTimeout, timerPort);
            indexExchangeTimeout = null;
            exchangeInProgress = false;

            try {
                for (IndexEntry indexEntry : event.getIndexEntries()) {
                    if (intersection.remove(new IndexHash(indexEntry)) && isIndexEntrySignatureValid(indexEntry)) {
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

    final Handler<IndexExchangeTimeout> handleIndexExchangeTimeout = new Handler<IndexExchangeTimeout>() {
        @Override
        public void handle(IndexExchangeTimeout event) {
            logger.debug(self.getId() + " index exchange timed out");
            indexExchangeTimeout = null;
            exchangeInProgress = false;

            publishUnresponsiveNodeDuringHashExchange();
        }
    };

    private void publishUnresponsiveNodeDuringHashExchange() {
        //to get nodes that didn't respond during index hash exchange
        HashSet<VodAddress> unresponsiveNodes = new HashSet<VodAddress>(nodesSelectedForIndexHashExchange);
        unresponsiveNodes.removeAll(nodesRespondedInIndexHashExchange);

        trigger(new FailureDetectorPort.FailureDetectorEvent(self.getAddress()), fdPort);

        for (VodAddress node : unresponsiveNodes) {
            publishUnresponsiveNode(node);
        }
    }

    private void publishUnresponsiveNode(VodAddress nodeAddress) {
        trigger(new FailureDetectorPort.FailureDetectorEvent(nodeAddress), fdPort);
    }

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
     * Add a new {link {@link IndexEntry} to the system and schedule a timeout
     * to wait for the acknowledgment.
     *
     * @param entry the {@link IndexEntry} to be added
     */
    private void addEntryGlobal(IndexEntry entry) {
        ScheduleTimeout rst = new ScheduleTimeout(config.getAddTimeout());
        rst.setTimeoutEvent(new AddIndexTimeout(rst, self.getId(), config.getRetryCount(), entry));
        addEntryGlobal(entry, rst);
    }

    /**
     * Add a new {link {@link IndexEntry} to the system, add the given timeout to the timer.
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
    final Handler<AddIndexEntryMessage.Request> handleAddIndexEntryRequest = new Handler<AddIndexEntryMessage.Request>() {
        @Override
        public void handle(AddIndexEntryMessage.Request event) {
            if (!leader || partitionInProgress) {
                return;
            }

            // TODO the recent requests list is a potential problem if the leader gets flooded with many requests
            if (recentRequests.containsKey(event.getTimeoutId())) {
                return;
            }

            Snapshot.incrementReceivedAddRequests();
            recentRequests.put(event.getTimeoutId(), System.currentTimeMillis());

            IndexEntry newEntry = event.getEntry();
            long id = getNextInsertionId();

            newEntry.setId(id);
            newEntry.setLeaderId(publicKey);
            newEntry.setGlobalId(java.util.UUID.randomUUID().toString());

            String signature = generateSignedHash(newEntry, privateKey);
            if (signature == null)
                return;

            newEntry.setHash(signature);

            trigger(new ViewSizeMessage.Request(event.getTimeoutId(), newEntry, event.getVodSource()), gradientRoutingPort);
        }
    };

    final Handler<ViewSizeMessage.Response> handleViewSizeResponse = new Handler<ViewSizeMessage.Response>() {
        @Override
        public void handle(ViewSizeMessage.Response response) {
            int viewSize = response.getViewSize();

            int majoritySize = (int) Math.ceil(viewSize / 2) + 1;

            //awaitingForPrepairResponse.put(response.getTimeoutId(), response.getNewEntry());
            replicationRequests.put(response.getTimeoutId(), new ReplicationCount(response.getSource(), majoritySize, response.getNewEntry()));

            trigger(new GradientRoutingPort.ReplicationPrepareCommitRequest(response.getNewEntry(), response.getTimeoutId()), gradientRoutingPort);

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
                Snapshot.incrementFailedddRequests();
                logger.warn("{} reached retry limit for adding a new entry {} ", self.getAddress(), event.entry);
                trigger(new UiAddIndexEntryResponse(false), uiPort);
            } else {

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
     * Stores on a peer in the leader group information about a new entry to be probably commited
     */
    final Handler<ReplicationPrepareCommitMessage.Request> handlePrepareCommit = new Handler<ReplicationPrepareCommitMessage.Request>() {
        @Override
        public void handle(ReplicationPrepareCommitMessage.Request request) {

            IndexEntry entry = request.getEntry();
            if (!isIndexEntrySignatureValid(entry) || !leaderIds.contains(entry.getLeaderId()))
                return;

            TimeoutId timeout = UUID.nextUUID();
            pendingForCommit.put(request.getEntry(), timeout);

            trigger(new ReplicationPrepareCommitMessage.Response(self.getAddress(), request.getVodSource(), request.getTimeoutId(), request.getEntry().getId()), networkPort);

            ScheduleTimeout rst = new ScheduleTimeout(config.getReplicationTimeout());
            rst.setTimeoutEvent(new AwaitingForCommitTimeout(rst, self.getId(), request.getEntry()));
            rst.getTimeoutEvent().setTimeoutId(timeout);
            trigger(rst, timerPort);
        }
    };

    /**
     * Clears rendingForCommit map as the entry wasn't commited on time
     */
    final Handler<AwaitingForCommitTimeout> handleAwaitingForCommitTimeout = new Handler<AwaitingForCommitTimeout>() {
        @Override
        public void handle(AwaitingForCommitTimeout awaitingForCommitTimeout) {
            if (pendingForCommit.containsKey(awaitingForCommitTimeout.getEntry()))
                pendingForCommit.remove(awaitingForCommitTimeout.getEntry());

        }
    };

    /**
     * Leader gains majority of responses and issues a request for a commit
     */
    final Handler<ReplicationPrepareCommitMessage.Response> handlePrepareCommitResponse = new Handler<ReplicationPrepareCommitMessage.Response>() {
        @Override
        public void handle(ReplicationPrepareCommitMessage.Response response) {
            TimeoutId timeout = response.getTimeoutId();

            ReplicationCount replicationCount = replicationRequests.get(timeout);
            if (replicationCount == null || !replicationCount.incrementAndCheckReceived())
                return;

            CancelTimeout ct = new CancelTimeout(timeout);
            trigger(ct, timerPort);

            IndexEntry entryToCommit = replicationCount.getEntry();
            TimeoutId commitTimeout = UUID.nextUUID();
            commitRequests.put(commitTimeout, replicationCount);
            replicationTimeoutToAdd.put(commitTimeout, response.getTimeoutId());
            replicationRequests.remove(timeout);

            ByteBuffer idBuffer = ByteBuffer.allocate(8);
            idBuffer.putLong(entryToCommit.getId());
            try {
                String signature = generateRSASignature(idBuffer.array(), privateKey);
                trigger(new GradientRoutingPort.ReplicationCommit(commitTimeout, entryToCommit.getId(), signature), gradientRoutingPort);

                ScheduleTimeout rst = new ScheduleTimeout(config.getReplicationTimeout());
                rst.setTimeoutEvent(new CommitTimeout(rst, self.getId()));
                rst.getTimeoutEvent().setTimeoutId(commitTimeout);
                trigger(rst, timerPort);
            } catch (NoSuchAlgorithmException e) {
                logger.error(self.getId() + " " + e.getMessage());
            } catch (InvalidKeyException e) {
                logger.error(self.getId() + " " + e.getMessage());
            } catch (SignatureException e) {
                logger.error(self.getId() + " " + e.getMessage());
            }
        }
    };

    /**
     * Performs commit on a peer in the leader group
     */
    final Handler<ReplicationCommitMessage.Request> handleCommitRequest = new Handler<ReplicationCommitMessage.Request>() {
        @Override
        public void handle(ReplicationCommitMessage.Request request) {
            long id = request.getEntryId();

            if (leaderIds.isEmpty())
                return;

            ByteBuffer idBuffer = ByteBuffer.allocate(8);
            idBuffer.putLong(id);
            try {
                if (!verifyRSASignature(idBuffer.array(), leaderIds.get(leaderIds.size() - 1), request.getSignature()))
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

            if (toCommit == null)
                return;

            CancelTimeout ct = new CancelTimeout(pendingForCommit.get(toCommit));
            trigger(ct, timerPort);

            try {
                addEntryLocal(toCommit);
                trigger(new ReplicationCommitMessage.Response(self.getAddress(), request.getVodSource(), request.getTimeoutId(), toCommit.getId()), networkPort);
                pendingForCommit.remove(toCommit);

                long maxStoredId = getMaxStoredId();

                ArrayList<Long> missingIds = new ArrayList<Long>();
                long currentMissingValue = maxStoredId < 0 ? 0 : maxStoredId + 1;
                while (currentMissingValue < toCommit.getId()) {
                    missingIds.add(currentMissingValue);
                    currentMissingValue++;
                }

                if (missingIds.size() > 0) {
                    RepairMessage.Request repairMessage = new RepairMessage.Request(self.getAddress(), request.getVodSource(), request.getTimeoutId(), missingIds.toArray(new Long[missingIds.size()]));
                    trigger(repairMessage, networkPort);
                }
            } catch (IOException e) {
                logger.error(self.getId() + " " + e.getMessage());
            } catch (LuceneAdaptorException e) {
                e.printStackTrace();
            }
        }
    };

    /**
     * Save entry on the leader and send an ACK back to the client
     * Only execute in the role of the leader. Garbage collect replication
     * requests if the constraints could not be satisfied in time. In this case,
     * no acknowledgment is sent to the client.
     */
    final Handler<ReplicationCommitMessage.Response> handleCommitResponse = new Handler<ReplicationCommitMessage.Response>() {
        @Override
        public void handle(ReplicationCommitMessage.Response response) {
            TimeoutId commitId = response.getTimeoutId();

            CancelTimeout ct = new CancelTimeout(commitId);
            trigger(ct, timerPort);

            if (!commitRequests.containsKey(commitId))
                return;

            ReplicationCount replicationCount = commitRequests.get(commitId);
            try {
                TimeoutId requestAddId = replicationTimeoutToAdd.get(commitId);
                if (requestAddId == null)
                    return;

                addEntryLocal(replicationCount.getEntry());

                trigger(new AddIndexEntryMessage.Response(self.getAddress(), replicationCount.getSource(), requestAddId), networkPort);

                replicationTimeoutToAdd.remove(commitId);
                commitRequests.remove(commitId);

                int partitionId = self.getPartitionId();

                Snapshot.addIndexEntryId(new Pair<Integer, Integer>(self.getCategoryId(), partitionId), replicationCount.getEntry().getId());
            } catch (IOException e) {
                logger.error(self.getId() + " " + e.getMessage());
            } catch (LuceneAdaptorException e) {
                e.printStackTrace();
            }
        }
    };

    final Handler<CommitTimeout> handleCommitTimeout = new Handler<CommitTimeout>() {
        @Override
        public void handle(CommitTimeout commitTimeout) {
            if (commitRequests.containsKey(commitTimeout.getTimeoutId())) {
                commitRequests.remove(commitTimeout.getTimeoutId());
                replicationTimeoutToAdd.remove(commitTimeout.getTimeoutId());
            }
        }
    };

    /**
     * An index entry has been successfully added.
     */
    final Handler<AddIndexEntryMessage.Response> handleAddIndexEntryResponse = new Handler<AddIndexEntryMessage.Response>() {
        @Override
        public void handle(AddIndexEntryMessage.Response event) {
            CancelTimeout ct = new CancelTimeout(event.getTimeoutId());
            trigger(ct, timerPort);

            Long timeStarted = timeStoringMap.get(event.getTimeoutId());
            if (timeStarted != null)
                Snapshot.reportAddingTime((new Date()).getTime() - timeStarted);

            timeStoringMap.remove(event.getTimeoutId());

            trigger(new UiAddIndexEntryResponse(true), uiPort);
        }
    };

    /**
     * Returns max stored id on a peer
     *
     * @return max stored id on a peer
     */
    private long getMaxStoredId() {
        long currentIndexValue = lowestMissingIndexValue - 1;

        if (existingEntries.isEmpty() || currentIndexValue > Collections.max(existingEntries))
            return currentIndexValue;

        return Collections.max(existingEntries);
    }

    /**
     * Handles situations then a peer in the leader group is behind in the updates during add operation
     * and asks for missing data
     */
    Handler<RepairMessage.Request> handleRepairRequest = new Handler<RepairMessage.Request>() {
        @Override
        public void handle(RepairMessage.Request request) {
            ArrayList<IndexEntry> missingEntries = new ArrayList<IndexEntry>();
            try {
                for (int i = 0; i < request.getMissingIds().length; i++) {
                    IndexEntry entry = findById(request.getMissingIds()[i]);
                    if (entry != null) missingEntries.add(entry);
                }
            } catch (IOException e) {
                logger.error(self.getId() + " " + e.getMessage());
            }

            RepairMessage.Response msg = new RepairMessage.Response(self.getAddress(), request.getVodSource(), request.getTimeoutId(), missingEntries);
            trigger(msg, networkPort);
        }
    };

    /**
     * Handles missing data on the peer from the leader group when adding a new entry, but the peer is behind
     * with the updates
     */
    Handler<RepairMessage.Response> handleRepairResponse = new Handler<RepairMessage.Response>() {
        @Override
        public void handle(RepairMessage.Response response) {
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
    final Handler<RecentRequestsGcTimeout> handleRecentRequestsGcTimeout = new Handler<RecentRequestsGcTimeout>() {
        @Override
        public void handle(RecentRequestsGcTimeout event) {
            long referenceTime = System.currentTimeMillis();

            ArrayList<TimeoutId> removeList = new ArrayList<TimeoutId>();
            for (TimeoutId id : recentRequests.keySet()) {
                if (referenceTime - recentRequests.get(id) > config
                        .getRecentRequestsGcInterval()) {
                    removeList.add(id);
                }
            }

            for (TimeoutId uuid : removeList) {
                recentRequests.remove(uuid);
            }
        }
    };

    /**
     * This handler listens to updates regarding the leader status
     */
    final Handler<LeaderStatusPort.LeaderStatus> handleLeaderStatus = new Handler<LeaderStatusPort.LeaderStatus>() {
        @Override
        public void handle(LeaderStatusPort.LeaderStatus event) {
            leader = event.isLeader();

            if (!leader) return;

            trigger(new PublicKeyBroadcast(publicKey), publicKeyPort);
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
        }

        ScheduleTimeout rst = new ScheduleTimeout(config.getQueryTimeout());
        rst.setTimeoutEvent(new SearchTimeout(rst, self.getId()));
        searchRequest.setTimeoutId((UUID) rst.getTimeoutEvent().getTimeoutId());
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

    /**
     * Query the local store with the given query string and send the response
     * back to the inquirer.
     */
    final Handler<SearchMessage.Request> handleSearchRequest = new Handler<SearchMessage.Request>() {
        @Override
        public void handle(SearchMessage.Request event) {
            try {
                ArrayList<IndexEntry> result = searchLocal(writeLuceneAdaptor, event.getPattern(), config.getHitsPerQuery());

                // Check the message and update the address in case of a Transport Protocol different than UDP.
                // Check the isSimulation flag inside the TransportHelper before running the code in simulations.
                SearchMessage.Response searchMessageResponse = new SearchMessage.Response(self.getAddress(), event.getVodSource(), event.getTimeoutId(), event.getSearchTimeoutId(), 0, 0, result, event.getPartitionId());
                TransportHelper.checkTransportAndUpdateBeforeSending(searchMessageResponse);
                trigger(searchMessageResponse, chunkManagerPort);

            } catch (IllegalSearchString illegalSearchString) {
                illegalSearchString.printStackTrace();
            } catch (LuceneAdaptorException e) {
                logger.warn("{} : Unable to search for index entries in Lucene.", self.getId());
                e.printStackTrace();
            }
        }
    };


    /**
     * Query the given index store with a given search pattern.
     *
     * @param index   the {@link Directory} to search in
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
     * Add the response to the search index store.
     */
    final Handler<SearchMessage.Response> handleSearchResponse = new Handler<SearchMessage.Response>() {
        @Override
        public void handle(SearchMessage.Response event) {

            // NOTE: For Simulation, check the simulation check inside the transport helper which should be true, for now.
            TransportHelper.checkTransportAndUpdateBeforeReceiving(event);
            if (searchRequest == null || !event.getSearchTimeoutId().equals(searchRequest.getTimeoutId())) {
                return;
            }
            addSearchResponse(event.getResults(), event.getPartitionId(), event.getSearchTimeoutId());
        }
    };

    /**
     * Add all entries from a {@link SearchMessage.Response} to the search index.
     *
     * @param entries   the entries to be added
     * @param partition the partition from which the entries originate from
     */
    private void addSearchResponse(Collection<IndexEntry> entries, int partition, TimeoutId requestId) {
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
        if (numOfPartitions == null)
            return;

        if (searchRequest.getNumberOfRespondedPartitions() == numOfPartitions) {
            logSearchTimeResults(requestId, System.currentTimeMillis(), numOfPartitions);
            CancelTimeout ct = new CancelTimeout(searchRequest.getTimeoutId());
            trigger(ct, timerPort);
            answerSearchRequest();
        }
    }

    private void logSearchTimeResults(TimeoutId requestId, long timeCompleted, Integer numOfPartitions) {
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
    final Handler<SearchTimeout> handleSearchTimeout = new Handler<SearchTimeout>() {
        @Override
        public void handle(SearchTimeout event) {
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

        CancelTimeout cancelTimeout = new CancelTimeout(indexExchangeTimeout);
        trigger(cancelTimeout, timerPort);
        indexExchangeTimeout = null;
        exchangeInProgress = false;

        int numberOfStoredIndexEntries = 0;
        try {

            if (isPartition) {
                deleteDocumentsWithIdMoreThen(writeLuceneAdaptor, middleId, minStoredId, maxStoredId);
                deleteHigherExistingEntries(middleId, existingEntries, false);
            } else {
                deleteDocumentsWithIdLessThen(writeLuceneAdaptor, middleId, minStoredId, maxStoredId);
                deleteLowerExistingEntries(middleId, existingEntries, true);
            }

            minStoredId = getMinStoredIdFromLucene(writeLuceneAdaptor);
            maxStoredId = getMaxStoredIdFromLucene(writeLuceneAdaptor);

            //Increment Max Store Id to keep in line with the original methodology.
            maxStoredId += 1;
            
            // Update the number of entries in the system.
            numberOfStoredIndexEntries = getTotalStoredEntriesCount(writeLuceneAdaptor);
                
        } catch (LuceneAdaptorException e) {
            logger.error("{}: Unable to cleanly remove the entries from the partition.", self.getId());
            e.printStackTrace();
        }
        self.setNumberOfIndexEntries(numberOfStoredIndexEntries);


        if (maxStoredId < minStoredId) {
            long temp = maxStoredId;
            maxStoredId = minStoredId;
            minStoredId = temp;
        }

        // TODO: The behavior of the lowestMissingIndex in case of the wrap around needs to be tested and some edge cases exists in this implementation.
        // FIXME: More cleaner solution is required.
        nextInsertionId = maxStoredId;
        lowestMissingIndexValue = (lowestMissingIndexValue < maxStoredId && lowestMissingIndexValue > minStoredId) ? lowestMissingIndexValue : maxStoredId;

        int partitionId = self.getPartitionId();

        Snapshot.resetPartitionLowestId(new Pair<Integer, Integer>(self.getCategoryId(), partitionId),
                minStoredId);
        Snapshot.resetPartitionHighestId(new Pair<Integer, Integer>(self.getCategoryId(), partitionId),
                maxStoredId);
        Snapshot.setNumIndexEntries(self.getAddress(), numberOfStoredIndexEntries);

        // It will ensure that the values of last missing index entries and other values are not getting updated.
        partitionInProgress = false;
    }


    /**
     * Modify the existingEntries set to remove the entries lower than mediaId.
     *
     * @param medianId
     * @param existingEntries
     * @param including
     */
    private void deleteLowerExistingEntries(Long medianId, Collection<Long> existingEntries, boolean including) {

        Iterator<Long> iterator = existingEntries.iterator();

        while (iterator.hasNext()) {
            Long currEntry = iterator.next();

            if (including) {
                if (currEntry.compareTo(medianId) <= 0)
                    iterator.remove();
            } else {
                if (currEntry.compareTo(medianId) < 0)
                    iterator.remove();
            }
        }
    }

    /**
     * Modify the exstingEntries set to remove the entries higher than median Id.
     *
     * @param medianId
     * @param existingEntries
     * @param including
     */
    private void deleteHigherExistingEntries(Long medianId, Collection<Long> existingEntries, boolean including) {

        Iterator<Long> iterator = existingEntries.iterator();

        while (iterator.hasNext()) {
            Long currEntry = iterator.next();

            if (including) {
                if (currEntry.compareTo(medianId) >= 0)
                    iterator.remove();
            } else {
                if (currEntry.compareTo(medianId) > 0)
                    iterator.remove();
            }
        }
    }


    /**
     * Add the given {@link IndexEntry}s to the given Lucene directory
     *
     * @param index   the directory to which the given entries should be added
     * @param entries a collection of index entries to be added
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
            logger.info("{} found {} entries for {}", new Object[]{self.getId(), result.size(), searchRequest.getSearchPattern()});

        } catch (LuceneAdaptorException e) {
            result = new ArrayList<IndexEntry>();  // In case of error set the result set as empty.
            logger.warn("{} : Unable to search for the entries.", self.getId());
            e.printStackTrace();
        } finally {
            trigger(new UiSearchResponse(result), uiPort);
        }
    }

    /**
     * Add the given {@link IndexEntry} to the given Lucene directory
     *
     * @param index the directory to which the given {@link IndexEntry} should be
     *              added
     * @param entry the {@link IndexEntry} to be added
     * @throws IOException in case the adding operation failed
     */
//    private void addIndexEntry(Directory index, IndexEntry entry) throws IOException, LuceneAdaptorException {
//
////        IndexWriter writer = new IndexWriter(index, indexWriterConfig);
//        addIndexEntry(writeLuceneAdaptor, entry);
////        writer.close();
//    }

    /**
     * Add the given {@link IndexEntry} to the Lucene index using the given
     * writer.
     *
     * @param writer the writer used to add the {@link IndexEntry}
     * @param entry  the {@link IndexEntry} to be added
     * @throws IOException in case the adding operation failed
     */
    private void addIndexEntry(LuceneAdaptor adaptor, IndexEntry entry) throws IOException, LuceneAdaptorException {
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
        Snapshot.incNumIndexEntries(self.getAddress());

        // Cancel gap detection timeouts for the given index
        TimeoutId timeoutId = gapTimeouts.get(indexEntry.getId());
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
        if (leader && self.getPartitionIdDepth() < config.getMaxPartitionIdLength())
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

            logger.info(" Partitioning Message Initiated at : " + self.getId() + " with Minimum Id: " + minStoredId + " and MaxStoreId: " + maxStoredId);
            partitionInProgress = true;
            trigger(new LeaderGroupInformation.Request((minStoredId + medianId), partitionsNumber, config.getLeaderGroupSize()), gradientRoutingPort);
        }


    }

    /**
     * Leader Group Information for the Two Phase Commit.
     */
    Handler<LeaderGroupInformation.Response> handlerLeaderGroupInformationResponse = new Handler<LeaderGroupInformation.Response>() {

        @Override
        public void handle(LeaderGroupInformation.Response event) {

            // TODO: Can be used to check if the responses are from the same sources as the requested ones ?
            List<VodAddress> leaderGroupAddresses = event.getLeaderGroupAddress();

            // Not enough nodes to continue.
            if (leaderGroupAddresses.size() < config.getLeaderGroupSize()) {
                logger.warn(" Not enough nodes to start the two phase commit.");
                partitionInProgress = false;
                return;
            }


            partitionRequestId = UUID.nextUUID();                    // The request Id to be associated with the partition request.
            TimeoutId timeoutId = UUID.nextUUID();                  // The id represents the current round id against which the responses will be checked for filtering.

            PartitionHelper.PartitionInfo partitionInfo = new PartitionHelper.PartitionInfo(event.getMedianId(), partitionRequestId, event.getPartitioningType());
            partitionInfo.setKey(publicKey);

            // Generate the hash information of the partition info for security purposes.
            String signedHash = generatePartitionInfoSignedHash(partitionInfo, privateKey);
            if (signedHash == null) {
                logger.error(" Signed Hash for the Two Phase Commit Is not getting generated.");
            }
            partitionInfo.setHash(signedHash);

            // Send the partition requests to the leader group.
            for (VodAddress destinationAddress : leaderGroupAddresses) {
                PartitionPrepareMessage.Request partitionPrepareRequest = new PartitionPrepareMessage.Request(self.getAddress(), destinationAddress, new OverlayId(self.getOverlayId()), timeoutId, partitionInfo);
                trigger(partitionPrepareRequest, networkPort);
            }

            // Set the timeout for the responses.
            // TODO: Add the timeout entry in the config for the update.
            ScheduleTimeout st = new ScheduleTimeout(config.getPartitionPrepareTimeout());
            PartitionPrepareMessage.Timeout pt = new PartitionPrepareMessage.Timeout(st, self.getId(), partitionInfo);

            st.setTimeoutEvent(pt);
            st.getTimeoutEvent().setTimeoutId(timeoutId);
            trigger(st, timerPort);

            // Create a replication object to track responses.
            PartitionReplicationCount count = new PartitionReplicationCount(config.getLeaderGroupSize(), partitionInfo);
            partitionPrepareReplicationCountMap.put(timeoutId, count);
        }
    };


    /**
     * Partition not successful, reset the information.
     */
    Handler<PartitionPrepareMessage.Timeout> partitionPrepareTimeoutHandler = new Handler<PartitionPrepareMessage.Timeout>() {

        @Override
        public void handle(PartitionPrepareMessage.Timeout event) {

            logger.warn(" Partition Timeout Occured. ");
            partitionInProgress = false;
            partitionPrepareReplicationCountMap.remove(event.getTimeoutId());

        }
    };


    /**
     * Handler for the PartitionPrepareRequest.
     */
    Handler<PartitionPrepareMessage.Request> handlerPartitionPrepareRequest = new Handler<PartitionPrepareMessage.Request>() {

        @Override
        public void handle(PartitionPrepareMessage.Request event) {


            // Step1: Verify that the data is from the leader only.
            if (!isPartitionUpdateValid(event.getPartitionInfo()) || !leaderIds.contains(event.getPartitionInfo().getKey())) {
                logger.error(" Partition Prepare Message Authentication Failed at: " + self.getId());
                return;
            }

            if (!partitionOrderValid(event.getOverlayId()))
                return;


            // Step2: Trigger the response for this request, which should be directly handled by the search component.
            PartitionPrepareMessage.Response response = new PartitionPrepareMessage.Response(self.getAddress(), event.getVodSource(), event.getTimeoutId(), event.getPartitionInfo().getRequestId());
            trigger(response, networkPort);


            // Step3: Add this to the map of pending partition updates.
            PartitionHelper.PartitionInfo receivedPartitionInfo = event.getPartitionInfo();
            TimeoutId timeoutId = UUID.nextUUID();
            partitionUpdatePendingCommit.put(receivedPartitionInfo, timeoutId);


            // Step4: Add timeout for this message.
            ScheduleTimeout st = new ScheduleTimeout(config.getPartitionCommitRequestTimeout());
            PartitionCommitTimeout pct = new PartitionCommitTimeout(st, self.getId(), event.getPartitionInfo());
            st.setTimeoutEvent(pct);
            st.getTimeoutEvent().setTimeoutId(timeoutId);
            trigger(st, timerPort);
        }
    };


    /**
     * This method basically prevents the nodes which rise quickly in the partition to avoid apply of updates, and apply the updates in order even though the update is being sent by the
     * leader itself. If not applied it screws up the min and max store id and lowestMissingIndexValues.
     * <p/>
     * DO NOT REMOVE THIS. (Prevents a rare fault case).
     *
     * @param source
     * @return applyPartitioningUpdate.
     */
    private boolean partitionOrderValid(OverlayId overlayId) {
        return (overlayId.getPartitioningType() == self.getPartitioningType() && overlayId.getPartitionIdDepth() == self.getPartitionIdDepth());
    }


    Handler<PartitionCommitTimeout> handlePartitionCommitTimeout = new Handler<PartitionCommitTimeout>() {

        @Override
        public void handle(PartitionCommitTimeout event) {

            logger.warn("(PartitionCommitTimeout): Didn't receive any information regarding commit so removing it from the list.");
            partitionUpdatePendingCommit.remove(event.getPartitionInfo());
        }
    };


    Handler<PartitionPrepareMessage.Response> handlerPartitionPrepareResponse = new Handler<PartitionPrepareMessage.Response>() {

        @Override
        public void handle(PartitionPrepareMessage.Response event) {

            // Step1: Filter the responses based on id's passed.
            TimeoutId receivedTimeoutId = event.getTimeoutId();
            PartitionReplicationCount count = partitionPrepareReplicationCountMap.get(receivedTimeoutId);

            if (partitionInProgress && count != null) {

                if (count.incrementAndCheckResponse(event.getVodSource())) {

                    // Received the required responses. Start the commit phase.
                    logger.debug("(PartitionPrepareMessage.Response): Time to start the commit phase. ");
                    List<VodAddress> leaderGroupAddress = count.getLeaderGroupNodesAddress();


                    // Cancel the prepare phase timeout as all the replies have been received.
                    CancelTimeout ct = new CancelTimeout(receivedTimeoutId);
                    trigger(ct, timerPort);

                    // Create a commit timeout.
                    TimeoutId commitTimeoutId = UUID.nextUUID();
                    ScheduleTimeout st = new ScheduleTimeout(config.getPartitionCommitTimeout());
                    PartitionCommitMessage.Timeout pt = new PartitionCommitMessage.Timeout(st, self.getId(), count.getPartitionInfo());
                    st.setTimeoutEvent(pt);
                    st.getTimeoutEvent().setTimeoutId(commitTimeoutId);

                    // Send the nodes commit messages with the commit timeoutid.
                    for (VodAddress dest : leaderGroupAddress) {
                        PartitionCommitMessage.Request partitionCommitRequest = new PartitionCommitMessage.Request(self.getAddress(), dest, commitTimeoutId, count.getPartitionInfo().getRequestId());
                        trigger(partitionCommitRequest, networkPort);
                    }

                    // Create a timeout for the responses. or Do we have to ?
                    // TODO: Not sure if this is required.

                    // Remove the data from the prepare map, to avoid handling of unnecessary messages.
                    partitionPrepareReplicationCountMap.remove(receivedTimeoutId);
                    count.resetLeaderGroupNodesAddress();

                    // Insert the data in the commit map.
                    partitionCommitReplicationCountMap.put(commitTimeoutId, count);         // Changing here to the committimeout id, not sure why it was running earlier.

                }
            }
        }
    };


    /**
     * Commit Phase Timeout Handler.
     */
    Handler<PartitionCommitMessage.Timeout> handlerPartitionCommitTimeoutMessage = new Handler<PartitionCommitMessage.Timeout>() {

        @Override
        public void handle(PartitionCommitMessage.Timeout event) {

            // Reset the partition flags
            logger.warn("Partition Commit Timeout Called at the leader");
            partitionInProgress = false;
            partitionCommitReplicationCountMap.remove(event.getTimeoutId());

        }
    };

    /**
     * Handler for the partition update commit.
     */
    Handler<PartitionCommitMessage.Request> handlerPartitionCommitRequest = new Handler<PartitionCommitMessage.Request>() {

        @Override
        public void handle(PartitionCommitMessage.Request event) {

            // Step1: Cancel the timeout as received the message on time.

            TimeoutId receivedPartitionRequestId = event.getPartitionRequestId();
            TimeoutId cancelTimeoutId = null;
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
            cancelTimeoutId = partitionUpdatePendingCommit.get(partitionUpdate);
            CancelTimeout cancelTimeout = new CancelTimeout(cancelTimeoutId);
            trigger(cancelTimeout, timerPort);


            LinkedList<PartitionHelper.PartitionInfo> partitionUpdates = new LinkedList<PartitionHelper.PartitionInfo>();
            partitionUpdates.add(partitionUpdate);

            // Apply the partition update.
            applyPartitioningUpdate(partitionUpdates);
            partitionUpdatePendingCommit.remove(partitionUpdate);               // Remove the partition update from the pending map.

            // Send a  conformation to the leader.
            PartitionCommitMessage.Response partitionCommitResponse = new PartitionCommitMessage.Response(self.getAddress(), event.getVodSource(), event.getTimeoutId(), event.getPartitionRequestId());
            trigger(partitionCommitResponse, networkPort);
        }
    };


    /**
     * Partition Commit Responses.
     */
    Handler<PartitionCommitMessage.Response> handlerPartitionCommitResponse = new Handler<PartitionCommitMessage.Response>() {
        @Override
        public void handle(PartitionCommitMessage.Response event) {

            // Filter responses based on current round.
            TimeoutId receivedTimeoutId = event.getTimeoutId();
            PartitionReplicationCount partitionReplicationCount = partitionCommitReplicationCountMap.get(receivedTimeoutId);

            if (partitionInProgress && partitionReplicationCount != null) {

                logger.debug("{PartitionCommitMessage.Response} received from the nodes at the Leader");

                // Partitioning complete ( Reset the partitioning parameters. )
//                partitionInProgress = false;
                // Reset the partitionInProgress when removed entries from your partition.

                leader = false;
                partitionCommitReplicationCountMap.remove(receivedTimeoutId);

                // Cancel the commit timeout.
                CancelTimeout ct = new CancelTimeout(event.getTimeoutId());
                trigger(ct, timerPort);

                logger.debug("Partitioning complete at the leader : " + self.getId());

                LinkedList<PartitionHelper.PartitionInfo> partitionUpdates = new LinkedList<PartitionHelper.PartitionInfo>();
                partitionUpdates.add(partitionReplicationCount.getPartitionInfo());

                // Inform the gradient about the partitioning.
                applyPartitioningUpdate(partitionUpdates);
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
        List<IndexEntry> indexEntries = findIdRange(writeLuceneAdaptor,id, id, 1);
        if (indexEntries.isEmpty()) {
            return null;
        }
        return indexEntries.get(0);
    }

    /**
     * Retrieve all indexes with ids in the given range from the local index
     * store.
     *
     * @param min   the inclusive minimum of the range
     * @param max   the inclusive maximum of the range
     * @param limit the maximal amount of entries to be returned
     * @return a list of the entries found
     * @throws IOException if Lucene errors occur
     */
    private List<IndexEntry> findIdRange(LuceneAdaptor adaptor, long min, long max, int limit) throws IOException {
        
        List<IndexEntry> indexEntries = new ArrayList<IndexEntry>();
        try {
            Query query = NumericRangeQuery.newLongRange(IndexEntry.ID, min, max, true, true);
            indexEntries = adaptor.searchIndexEntriesInLucene(query,  new Sort(new SortField(IndexEntry.ID,Type.LONG)), limit);
            
        } catch (LuceneAdaptorException e) {
            e.printStackTrace();
            logger.error("{}: Exception while trying to fetch the index entries between specified range.", self.getId());
        }
        
        return indexEntries;
    }

    /**
     * Generates a SHA-1 hash on IndexEntry and signs it with a private key
     *
     * @param newEntry
     * @return signed SHA-1 key
     */
    private static String generateSignedHash(IndexEntry newEntry, PrivateKey privateKey) {
        if (newEntry.getLeaderId() == null)
            return null;

        //url
        ByteBuffer dataBuffer = getByteDataFromIndexEntry(newEntry);


        try {
            return generateRSASignature(dataBuffer.array(), privateKey);
        } catch (NoSuchAlgorithmException e) {
            logger.error(e.getMessage());
        } catch (SignatureException e) {
            logger.error(e.getMessage());
        } catch (InvalidKeyException e) {
            logger.error(e.getMessage());
        }

        return null;
    }


    /**
     * Generates the SHA-1 Hash Of the partition update and sign with private key.
     *
     * @param partitionInfo
     * @param privateKey
     * @return signed hash
     */
    private String generatePartitionInfoSignedHash(PartitionHelper.PartitionInfo partitionInfo, PrivateKey privateKey) {

        if (partitionInfo.getKey() == null)
            return null;

        // generate the byte array from the partitioning data.
        ByteBuffer byteBuffer = getByteDataFromPartitionInfo(partitionInfo);

        // sign the array and return the signed hash value.
        try {
            return generateRSASignature(byteBuffer.array(), privateKey);
        } catch (NoSuchAlgorithmException e) {
            logger.error(e.getMessage());
        } catch (SignatureException e) {
            logger.error(e.getMessage());
        } catch (InvalidKeyException e) {
            logger.error(e.getMessage());
        }

        return null;
    }


    /**
     * Generate the SHA-1 String.
     * TODO: For more efficiency don't convert it to string as it becomes greater than 256bytes and encoding mechanism fails for index hash exchange.
     * FIXME: Change the encoding hash mechanism.
     *
     * @param data
     * @param privateKey
     * @return
     * @throws NoSuchAlgorithmException
     * @throws InvalidKeyException
     * @throws SignatureException
     */
    private static String generateRSASignature(byte[] data, PrivateKey privateKey) throws NoSuchAlgorithmException, InvalidKeyException, SignatureException {
        MessageDigest digest = MessageDigest.getInstance("SHA-1");
        String sha1 = byteArray2Hex(digest.digest(data));

        Signature instance = Signature.getInstance("SHA1withRSA");
        instance.initSign(privateKey);
        instance.update(sha1.getBytes());
        byte[] signature = instance.sign();
        return byteArray2Hex(signature);
    }

    private static String byteArray2Hex(final byte[] hash) {
        Formatter formatter = new Formatter();
        for (byte b : hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private static boolean isIndexEntrySignatureValid(IndexEntry newEntry) {
        if (newEntry.getLeaderId() == null)
            return false;
        ByteBuffer dataBuffer = getByteDataFromIndexEntry(newEntry);


        try {
            return verifyRSASignature(dataBuffer.array(), newEntry.getLeaderId(), newEntry.getHash());
        } catch (NoSuchAlgorithmException e) {
            logger.error(e.getMessage());
        } catch (SignatureException e) {
            logger.error(e.getMessage());
        } catch (InvalidKeyException e) {
            logger.error(e.getMessage());
        }

        return false;
    }


    /**
     * Verify if the partition update is received from the leader itself only.
     *
     * @param partitionUpdate
     * @return
     */
    private static boolean isPartitionUpdateValid(PartitionHelper.PartitionInfo partitionUpdate) {

        if (partitionUpdate.getKey() == null)
            return false;

        ByteBuffer dataBuffer = getByteDataFromPartitionInfo(partitionUpdate);

        try {
            return verifyRSASignature(dataBuffer.array(), partitionUpdate.getKey(), partitionUpdate.getHash());
        } catch (NoSuchAlgorithmException e) {
            logger.error(e.getMessage());
        } catch (SignatureException e) {
            logger.error(e.getMessage());
        } catch (InvalidKeyException e) {
            logger.error(e.getMessage());
        }

        return false;

    }


    private static ByteBuffer getByteDataFromIndexEntry(IndexEntry newEntry) {
        //url
        byte[] urlBytes;
        if (newEntry.getUrl() != null)
            urlBytes = newEntry.getUrl().getBytes(Charset.forName("UTF-8"));
        else
            urlBytes = new byte[0];

        //filename
        byte[] fileNameBytes;
        if (newEntry.getFileName() != null)
            fileNameBytes = newEntry.getFileName().getBytes(Charset.forName("UTF-8"));
        else
            fileNameBytes = new byte[0];

        //language
        byte[] languageBytes;
        if (newEntry.getLanguage() != null)
            languageBytes = newEntry.getLanguage().getBytes(Charset.forName("UTF-8"));
        else
            languageBytes = new byte[0];

        //description
        byte[] descriptionBytes;
        if (newEntry.getDescription() != null)
            descriptionBytes = newEntry.getDescription().getBytes(Charset.forName("UTF-8"));
        else
            descriptionBytes = new byte[0];

        ByteBuffer dataBuffer;
        if (newEntry.getUploaded() != null)
            dataBuffer = ByteBuffer.allocate(8 * 3 + 4 + urlBytes.length + fileNameBytes.length +
                    languageBytes.length + descriptionBytes.length);
        else
            dataBuffer = ByteBuffer.allocate(8 * 2 + 4 + urlBytes.length + fileNameBytes.length +
                    languageBytes.length + descriptionBytes.length);
        dataBuffer.putLong(newEntry.getId());
        dataBuffer.putLong(newEntry.getFileSize());
        if (newEntry.getUploaded() != null)
            dataBuffer.putLong(newEntry.getUploaded().getTime());
        dataBuffer.putInt(newEntry.getCategory().ordinal());
        if (newEntry.getUrl() != null)
            dataBuffer.put(urlBytes);
        if (newEntry.getFileName() != null)
            dataBuffer.put(fileNameBytes);
        if (newEntry.getLanguage() != null)
            dataBuffer.put(languageBytes);
        if (newEntry.getDescription() != null)
            dataBuffer.put(descriptionBytes);
        return dataBuffer;
    }

    /**
     * Converts the partitioning update in byte array.
     *
     * @param paritionInfo
     * @return partitionInfo byte array.
     */
    private static ByteBuffer getByteDataFromPartitionInfo(PartitionHelper.PartitionInfo partitionInfo) {

        // Decide on a specific order.
        ByteBuffer buffer = ByteBuffer.allocate(8 + (2 * 4));

        // Start filling the buffer with information.
        buffer.putLong(partitionInfo.getMedianId());
        if (partitionInfo.getRequestId() instanceof NoTimeoutId)
            buffer.putInt(-1);
        else
            buffer.putInt(partitionInfo.getRequestId().getId());

        buffer.putInt(partitionInfo.getPartitioningTypeInfo().ordinal());


        return buffer;
    }

    private static boolean verifyRSASignature(byte[] data, PublicKey key, String signature) throws NoSuchAlgorithmException, InvalidKeyException, SignatureException {
        MessageDigest digest = MessageDigest.getInstance("SHA-1");
        String sha1 = byteArray2Hex(digest.digest(data));

        Signature instance = Signature.getInstance("SHA1withRSA");
        instance.initVerify(key);
        instance.update(sha1.getBytes());
        return instance.verify(hexStringToByteArray(signature));
    }

    public static byte[] hexStringToByteArray(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                    + Character.digit(s.charAt(i + 1), 16));
        }
        return data;
    }

    /**
     * Returns min id value stored in Lucene
     *
     * @return min Id value stored in Lucene
     */
    private long getMinStoredIdFromLucene(LuceneAdaptor adaptor) throws LuceneAdaptorException {
        
        long minStoreId =0;
        Query query = NumericRangeQuery.newLongRange(IndexEntry.ID, Long.MIN_VALUE, Long.MAX_VALUE, true, true);
        int numofEntries = 1;
        
        Sort sort =  new Sort(new SortField(IndexEntry.ID, Type.LONG));
        List<IndexEntry> indexEntries = adaptor.searchIndexEntriesInLucene(query, sort, numofEntries);

        if (indexEntries.size() == 1) {
            minStoreId = indexEntries.get(0).getId();
        }
        return minStoreId;
    }

    /**
     * Returns max id value stored in Lucene
     *
     * @return max Id value stored in Lucene
     */
    private long getMaxStoredIdFromLucene(LuceneAdaptor adaptor) throws LuceneAdaptorException {

        long maxStoreId =0;
        Query query = NumericRangeQuery.newLongRange(IndexEntry.ID, Long.MIN_VALUE, Long.MAX_VALUE, true, true);
        int numofEntries = 1;
        Sort sort =  new Sort(new SortField(IndexEntry.ID, Type.LONG, true));
        List<IndexEntry> indexEntries = adaptor.searchIndexEntriesInLucene(query, sort, numofEntries);

        if (indexEntries.size() == 1) {
            maxStoreId = indexEntries.get(0).getId();
        }
        return maxStoreId;
    }

    /**
     * Deletes all documents from the index with ids less or equal then id
     *
     * @param id
     * @param bottom
     * @param top
     * @return
     */
    private void deleteDocumentsWithIdLessThen(LuceneAdaptor adaptor, long id, long bottom, long top) throws LuceneAdaptorException {

        if (bottom < top) {
            Query query = NumericRangeQuery.newLongRange(IndexEntry.ID, bottom, id, true, true);
            adaptor.deleteDocumentsFromLucene(query);
        } else {
            if (id < bottom) {
                Query query1 = NumericRangeQuery.newLongRange(IndexEntry.ID, bottom, Long.MAX_VALUE - 1, true, true);
                Query query2 = NumericRangeQuery.newLongRange(IndexEntry.ID, Long.MIN_VALUE + 1, id, true, true);
                adaptor.deleteDocumentsFromLucene(query1, query2);
            } else {
                Query query = NumericRangeQuery.newLongRange(IndexEntry.ID, bottom, id, true, true);
                adaptor.deleteDocumentsFromLucene(query);
            }
        }

    }

    /**
     * Deletes all documents from the index with ids bigger then id (not including)
     *
     * @param id
     * @param bottom
     * @param top
     */
    private void deleteDocumentsWithIdMoreThen(LuceneAdaptor adaptor, long id, long bottom, long top) throws LuceneAdaptorException {


        if (bottom < top) {
            Query query = NumericRangeQuery.newLongRange(IndexEntry.ID, id + 1, top, true, true);
            adaptor.deleteDocumentsFromLucene(query);

        } else {
            if (id >= top) {
                Query query1 = NumericRangeQuery.newLongRange(IndexEntry.ID, id + 1, Long.MAX_VALUE - 1, true, true);
                Query query2 = NumericRangeQuery.newLongRange(IndexEntry.ID, Long.MIN_VALUE + 1, top, true, true);
                adaptor.deleteDocumentsFromLucene(query1, query2);
            } else {
                Query query = NumericRangeQuery.newLongRange(IndexEntry.ID, id + 1, top, true, true);
                adaptor.deleteDocumentsFromLucene(query);
            }
        }
    }


    /**
     * In case the dynamic utilities, if a leader partition, then it might happen that its utility falls below the nodes which have not yet partitioned.
     * Thus, it starts asking from the nodes which have not yet partitioned for the updates.
     *
     * @return true in case the message from node ahead in terms of partitioning.
     */
    private boolean isMessageFromNodeLaggingBehind(VodAddress address) {

        boolean result = false;

        // Construct an overlay address from the VodAdress.
        OverlayAddress receivedOverlayAddress = new OverlayAddress(address);

        // Create an object about your own overlay address.
        OverlayAddress selfOverlayAddress = new OverlayAddress(self.getAddress());

        // Only go deep into checking if the overlay ids are different.
        if (!receivedOverlayAddress.getOverlayId().equals(selfOverlayAddress.getOverlayId())) {

            // Move ahead only in case I have partitioned, else accept the message from other nodes.
            if (selfOverlayAddress.getPartitioningType() != VodAddress.PartitioningType.NEVER_BEFORE) {

                // Difference should never be less than or equal to zero as nodes start with partitioning depth =1.
                if ((selfOverlayAddress.getPartitionIdDepth() - receivedOverlayAddress.getPartitionIdDepth()) > 0) {
                    result = true;
                }
            }
        }

        return result;
    }

    /**
     * Apply the partitioning updates received.
     */
    public void applyPartitioningUpdate(LinkedList<PartitionHelper.PartitionInfo> partitionUpdates) {

        for (PartitionHelper.PartitionInfo update : partitionUpdates) {

            boolean duplicateFound = false;
            for (PartitionHelper.PartitionInfo partitionInfo : partitionHistory) {
                if (partitionInfo.getRequestId().getId() == update.getRequestId().getId()) {
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
            trigger(new LeaderStatusPort.TerminateBeingLeader(), leaderStatusPort);

//            trigger(new StatusAggregatorMessages.SearchUpdateEvent(new SearchStatusData()), statusAggregatorPortPositive);
        }
    }

    /**
     * Based on the source address, provide the control message enum that needs to be associated with the control response object.
     */
    private ControlMessageEnum fetchPartitioningHashUpdatesMessageEnum(VodAddress address, OverlayId overlayId, List<PartitionHelper.PartitionInfoHash> partitionUpdateHashes) {

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

            int myDepth = self.getPartitionIdDepth();
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
                    self.getPartitionIdDepth() + 1, newPartitionId, selfCategory);
            ((MsSelfImpl) self).setOverlayId(newOverlayId);
        }
        logger.debug("Partitioning Occurred at Node: " + self.getId() + " PartitionDepth: " + self.getPartitionIdDepth() + " PartitionId: " + self.getPartitionId() + " PartitionType: " + self.getPartitioningType());
        int partitionId = self.getPartitionId();
        Snapshot.updateInfo(self.getAddress());                 // Overlay id present in the snapshot not getting updated, so added the method.
        Snapshot.addPartition(new Pair<Integer, Integer>(self.getCategoryId(), partitionId));
        return partitionSubId;
    }

    /**
     * Based on the unique ids return the partition updates back.
     *
     * @param partitionUpdatesIds
     * @return
     */
    public LinkedList<PartitionHelper.PartitionInfo> fetchPartitioningUpdates(List<TimeoutId> partitionUpdatesIds) {

        LinkedList<PartitionHelper.PartitionInfo> partitionUpdates = new LinkedList<PartitionHelper.PartitionInfo>();

        // Iterate over the available partition updates.
        for (TimeoutId partitionUpdateId : partitionUpdatesIds) {

            boolean found = false;
            for (PartitionHelper.PartitionInfo partitionInfo : partitionHistory) {
                if (partitionInfo.getRequestId().equals(partitionUpdateId)) {
                    partitionUpdates.add(partitionInfo);
                    found = true;
                    break;
                }
            }

            if (!found) {
                // Stop The addition as there has been a missing piece.
                break;
            }
        }

        // Return the ordered update list.
        return partitionUpdates;
    }

    /**
     * Request To check if the source address is behind in terms of partitioning updates.
     */
    private CheckPartitionInfoHashUpdate.Response getCheckPartitionInfoHashUpdateResponse(ControlMessage.Request event) {

        // Check for the partitioning updates and return it back.
        logger.debug("Check Partitioning Update Received.");

        LinkedList<PartitionHelper.PartitionInfoHash> partitionUpdateHashes = new LinkedList<PartitionHelper.PartitionInfoHash>();
        ControlMessageEnum controlMessageEnum;

        // Check for the responses when you have atleast partitioned yourself.
        if (self.getPartitioningType() != VodAddress.PartitioningType.NEVER_BEFORE) {

            controlMessageEnum = fetchPartitioningHashUpdatesMessageEnum(event.getVodSource(), event.getOverlayId(), partitionUpdateHashes);
        } else {
            // Send empty partition update as you have not partitioned.
            controlMessageEnum = ControlMessageEnum.PARTITION_UPDATE;
        }

        // Trigger the partitioning update.
        return new CheckPartitionInfoHashUpdate.Response(event.getRoundId(), event.getVodSource(), partitionUpdateHashes, controlMessageEnum);
    }


    /**
     * Push updated information to the listening components.
     *
     * @param self Updated Self
     */
    private void informListeningComponentsAboutUpdates(MsSelfImpl self){

        SearchDescriptor updatedDesc = new SearchDescriptor(new OverlayAddress(self.getAddress()), 0, false, self.getNumberOfIndexEntries(), self.getPartitionIdDepth());
        
        trigger(new SelfChangedPort.SelfChangedEvent(self), selfChangedPort);
        trigger(new CroupierUpdate(java.util.UUID.randomUUID(), updatedDesc), croupierPortPositive);
        trigger(new SearchComponentUpdateEvent(new SearchComponentUpdate(updatedDesc, defaultComponentOverlayId)), statusAggregatorPortPositive);
        trigger(new GradientUpdate(updatedDesc), gradientPort);
    }


    /**
     * 
     * Handle the sample from the gradient.
     */
    Handler<GradientSample> gradientSampleHandler = new Handler<GradientSample>() {
        @Override
        public void handle(GradientSample event) {
            logger.info("Received Gradient Sample");
                    
        }
    };
    

}

