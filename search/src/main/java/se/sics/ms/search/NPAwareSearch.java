package se.sics.ms.search;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.*;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.kth.ms.partitionaware.api.port.PALPort;
import se.sics.gvod.config.SearchConfiguration;
import se.sics.gvod.net.VodAddress;
import se.sics.kompics.*;
import se.sics.kompics.network.Network;
import se.sics.kompics.network.Transport;
import se.sics.kompics.timer.*;
import se.sics.kompics.timer.Timer;
import se.sics.ms.aggregator.port.StatusAggregatorPort;
import se.sics.ms.common.*;
import se.sics.ms.configuration.MsConfig;
import se.sics.ms.data.*;
import se.sics.ms.data.aggregator.ElectionLeaderComponentUpdate;
import se.sics.ms.data.aggregator.ElectionLeaderUpdateEvent;
import se.sics.ms.data.aggregator.SearchComponentUpdate;
import se.sics.ms.data.aggregator.SearchComponentUpdateEvent;
import se.sics.ms.events.*;
import se.sics.ms.gradient.events.*;
import se.sics.ms.gradient.ports.GradientRoutingPort;
import se.sics.ms.gradient.ports.LeaderStatusPort;
import se.sics.ms.model.LocalSearchRequest;
import se.sics.ms.model.ReplicationCount;
import se.sics.ms.ports.SelfChangedPort;
import se.sics.ms.ports.SimulationEventsPort;
import se.sics.ms.ports.SimulationEventsPort.AddIndexSimulated;
import se.sics.ms.ports.UiPort;
import se.sics.ms.timeout.AwaitingForCommitTimeout;
import se.sics.ms.types.*;
import se.sics.ms.util.*;
import se.sics.ms.util.Pair;
import se.sics.p2ptoolbox.election.api.msg.*;
import se.sics.p2ptoolbox.election.api.ports.LeaderElectionPort;
import se.sics.p2ptoolbox.gradient.GradientPort;
import se.sics.p2ptoolbox.gradient.msg.GradientSample;
import se.sics.p2ptoolbox.gradient.msg.GradientUpdate;
import se.sics.p2ptoolbox.util.Container;
import se.sics.p2ptoolbox.util.network.impl.BasicAddress;
import se.sics.p2ptoolbox.util.network.impl.BasicContentMsg;
import se.sics.p2ptoolbox.util.network.impl.DecoratedAddress;
import se.sics.p2ptoolbox.util.network.impl.DecoratedHeader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.*;
import java.util.*;


/**
 * This class handles the storing, adding and searching for indexes. It acts in
 * two different modes depending on if it the executing node was elected leader
 * or not.
 * <p/>
 * {@link se.sics.ms.types.IndexEntry}s are spread via gossiping using the Cyclon samples stored
 * in the routing tables for the partition of the local node.
 */
public final class NPAwareSearch extends ComponentDefinition {
    /**
     * Set to true to store the Lucene index on disk
     */
    public static final boolean PERSISTENT_INDEX = false;

    // ====== PORTS.
    Positive<SimulationEventsPort> simulationEventsPort = positive(SimulationEventsPort.class);
    Positive<Network> networkPort = positive(Network.class);
    Positive<Timer> timerPort = positive(Timer.class);
    Positive<GradientRoutingPort> gradientRoutingPort = positive(GradientRoutingPort.class);
    Negative<LeaderStatusPort> leaderStatusPort = negative(LeaderStatusPort.class);
    Negative<UiPort> uiPort = negative(UiPort.class);
    Negative<SelfChangedPort> selfChangedPort = negative(SelfChangedPort.class);
    Positive<StatusAggregatorPort> statusAggregatorPortPositive = requires(StatusAggregatorPort.class);
    Positive<GradientPort> gradientPort = requires(GradientPort.class);
    Positive<LeaderElectionPort> electionPort = requires(LeaderElectionPort.class);
    Positive<PALPort> pagPort = requires(PALPort.class);

    // ======== LOCAL VARIABLES.

    private static final Logger logger = LoggerFactory.getLogger(NPAwareSearch.class);
    private String prefix;
    private long seed;
    private ApplicationSelf self;
    private SearchConfiguration config;
    private boolean leader;
    private long lowestMissingIndexValue;
    private SortedSet<Long> existingEntries;
    private long nextInsertionId;
    private long currentEpoch = 0;
    private boolean markerEntryAdded = false;
    private TreeSet<PeerDescriptor> gradientEntrySet;
    private DecoratedAddress leaderAddress;
    private PublicKey leaderKey;

    private Map<UUID, ReplicationCount> replicationRequests;
    private Map<UUID, Long> recentRequests;

    // Apache Lucene used for searching
    private StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_42);
    private Directory index;
    private IndexWriterConfig indexWriterConfig = new IndexWriterConfig(Version.LUCENE_42, analyzer);

    // Lucene variables used to store and search in collected answers
    private LocalSearchRequest searchRequest;
    private Directory searchIndex;


    // Leader Election Protocol.
    private UUID electionRound = UUID.randomUUID();

    // Aggregator Variable.
    private int defaultComponentOverlayId = 0;

    private PrivateKey privateKey;
    private PublicKey publicKey;
    private ArrayList<PublicKey> leaderIds = new ArrayList<PublicKey>();
    private HashMap<ApplicationEntry, org.javatuples.Pair<UUID, LeaderUnit>> pendingForCommit = new HashMap<ApplicationEntry, org.javatuples.Pair<UUID, LeaderUnit>>();
    private HashMap<UUID, Integer> searchPartitionsNumber = new HashMap<UUID, Integer>();

    private HashMap<UUID, Long> timeStoringMap = new HashMap<UUID, Long>();
    private static HashMap<UUID, Pair<Long, Integer>> searchRequestStarted = new HashMap<UUID, Pair<Long, Integer>>();

    // Partitioning Protocol Information.
    private boolean partitionInProgress = false;

    // Generic Control Pull Mechanism.
    private IndexEntryLuceneAdaptor searchRequestLuceneAdaptor;
    private ApplicationLuceneAdaptor searchEntryLuceneAdaptor;
    private ApplicationLuceneAdaptor writeEntryLuceneAdaptor;
    private MarkerEntryLuceneAdaptor markerEntryLuceneAdaptor;
    private LowestMissingEntryTracker lowestMissingEntryTracker;
    private SearchProtocolTracker searchProtocolTracker;

    // Leader Election Protocol.
    private Collection<DecoratedAddress> leaderGroupInformation;

    // Trackers.
    private MultipleEntryAdditionTracker entryAdditionTracker;
    private Map<UUID, UUID> entryPrepareTimeoutMap; // (roundId, prepareTimeoutId).
    private LandingEntryTracker landingEntryTracker;
    private ControlPullTracker controlPullTracker;
    private ShardTracker shardTracker;
    private TimeLine timeLine;
    private Comparator<LeaderUnit> luComparator = new GenericECComparator();
    private Comparator<ApplicationEntry> entryComparator = new AppEntryComparator();
    private PeerDescriptor selfDescriptor;
    private UUID preShardTimeoutId;
    private List<LeaderUnit> bufferedUnits;


    // Pagination
    SearchCache cache;




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
         * @return the {@link se.sics.ms.types.IndexEntry} this timeout was scheduled for
         */
        public IndexEntry getEntry() {
            return entry;
        }
    }

    public NPAwareSearch(SearchInit init) {

        doInit(init);
        subscribe(handleStart, control);
        subscribe(handleAddIndexSimulated, simulationEventsPort);
        subscribe(handleAddIndexEntryRequest, networkPort);
        subscribe(preparePhaseTimeout, timerPort);

        subscribe(handleAddIndexEntryResponse, networkPort);
        subscribe(handleSearchRequest, networkPort);
        subscribe(handleSearchResponse, networkPort);
        subscribe(handleSearchTimeout, timerPort);

        subscribe(landingEntryAddTimeout, timerPort);
        subscribe(handleAddRequestTimeout, timerPort);
        subscribe(handleRecentRequestsGcTimeout, timerPort);
        subscribe(searchRequestHandler, uiPort);

        subscribe(handleEntryAddPrepareRequest, networkPort);
        subscribe(handleLandingEntryAddPrepareRequest, networkPort);
        subscribe(handleAwaitingForCommitTimeout, timerPort);

        subscribe(handleEntryAdditionPrepareResponse, networkPort);
        subscribe(handleEntryCommitRequest, networkPort);
        subscribe(addIndexEntryRequestHandler, uiPort);

        subscribe(handleSearchSimulated, simulationEventsPort);
        subscribe(handleNumberOfPartitions, gradientRoutingPort);

        // Generic Control Pull Exchange With Epochs.
        subscribe(controlPullTracker.exchangeRoundHandler, timerPort);
        subscribe(controlPullTracker.controlPullRequest, networkPort);
        subscribe(controlPullTracker.controlPullResponse, networkPort);

        // LeaderElection handlers.
        subscribe(leaderElectionHandler, electionPort);
        subscribe(terminateBeingLeaderHandler, electionPort);
        subscribe(leaderUpdateHandler, electionPort);
        subscribe(enableLGMembershipHandler, electionPort);
        subscribe(disableLGMembershipHandler, electionPort);
        subscribe(extensionUpdateHandler, electionPort);

        // Updated Missing tracker information.
        subscribe(lowestMissingEntryTracker.entryExchangeRoundHandler, timerPort);
        subscribe(lowestMissingEntryTracker.entryHashExchangeRequestHandler, networkPort);
        subscribe(lowestMissingEntryTracker.entryHashExchangeResponseHandler, networkPort);
        subscribe(lowestMissingEntryTracker.entryExchangeRequestHandler, networkPort);
        subscribe(lowestMissingEntryTracker.entryExchangeResponseHandler, networkPort);
        subscribe(lowestMissingEntryTracker.leaderPullRequest, networkPort);
        subscribe(lowestMissingEntryTracker.leaderPullResponse, networkPort);

        // Shard Protocol Handlers
        subscribe(shardRoundTimeoutHandler, timerPort);
        subscribe(shardTracker.shardingPrepareRequest, networkPort);
        subscribe(shardTracker.shardingPrepareResponse, networkPort);
        subscribe(shardTracker.shardingCommitRequest, networkPort);
        subscribe(shardTracker.shardingCommitResponse, networkPort);
        subscribe(shardTracker.awaitingShardCommitHandler, timerPort);

        subscribe(gradientSampleHandler, gradientPort);
        subscribe(preShardTimeoutHandler, timerPort);
        
//      PAL Handlers
        
//        subscribe(leaderUnitCheckHandler, pagPort);
//        subscribe(npTimeoutHandler, timerPort);

        // PAGINATION.
        subscribe(searchProtocolTracker.numPartitionsHandler, gradientRoutingPort);
        subscribe(searchProtocolTracker.handleSearchQueryRequest, networkPort);
        subscribe(searchProtocolTracker.handleSearchQueryResponse, networkPort);
        subscribe(searchProtocolTracker.handleSearchFetchRequest, networkPort);
        subscribe(searchProtocolTracker.handleSearchFetchResponse, networkPort);
        subscribe(searchProtocolTracker.searchProtocolTimeout, timerPort);
        subscribe(searchProtocolTracker.cacheTimeoutHandler, timerPort);
    }

    /**
     * Initialize the component.
     */
    private void doInit(SearchInit init) {

        seed = init.getSeed();
        self = init.getSelf();
        prefix = String.valueOf(self.getId());
        config = init.getConfiguration();
        publicKey = init.getPublicKey();
        privateKey = init.getPrivateKey();
        gradientEntrySet = new TreeSet<PeerDescriptor>();

        replicationRequests = new HashMap<UUID, ReplicationCount>();
        recentRequests = new HashMap<UUID, Long>();
        nextInsertionId = ApplicationConst.STARTING_ENTRY_ID;
        lowestMissingIndexValue = 0;
        existingEntries = new TreeSet<Long>();
        bufferedUnits = new ArrayList<LeaderUnit>();

        searchRequest = new LocalSearchRequest();
        cache = new SearchCache();
        // Trackers.
        initializeTrackers();
        index = new RAMDirectory();
        setupApplicationLuceneWriter(index, indexWriterConfig);
        setupMarkerLuceneWriter(index, indexWriterConfig);
    }


    /**
     * Initialize the trackers to be used in the application.
     */
    private void initializeTrackers() {

        entryAdditionTracker = new MultipleEntryAdditionTracker(100); // Can hold upto 100 simultaneous requests.
        entryPrepareTimeoutMap = new HashMap<UUID, UUID>();
        landingEntryTracker = new LandingEntryTracker();
        lowestMissingEntryTracker = new LowestMissingEntryTracker();
        controlPullTracker = new ControlPullTracker();
        shardTracker = new ShardTracker();
        timeLine = new TimeLine();
        searchProtocolTracker = new SearchProtocolTracker();

    }

    /**
     * Based on the information provided, create a lucene writer for adding application
     * entries in the system.
     *
     * @param index             Directory
     * @param indexWriterConfig Index Writer Configuration.
     */
    private void setupApplicationLuceneWriter(Directory index, IndexWriterConfig indexWriterConfig) {
        try {
            writeEntryLuceneAdaptor = new ApplicationLuceneAdaptorImpl(index, indexWriterConfig);
            writeEntryLuceneAdaptor.initialEmptyWriterCommit();
        } catch (LuceneAdaptorException e) {
            e.printStackTrace();
            throw new RuntimeException(" Unable to open index for Lucene");
        }
    }


    /**
     * Create Lucene Writer for pushing Marker Entries in the system.
     *
     * @param index index
     * @param indexWriterConfig config
     */
    private void setupMarkerLuceneWriter(Directory index, IndexWriterConfig indexWriterConfig) {
        
        try{
            markerEntryLuceneAdaptor = new MarkerEntryLuceneAdaptorImpl(index, indexWriterConfig);
            markerEntryLuceneAdaptor.initialEmptyWriterCommit();
            
        } catch (LuceneAdaptorException e) {
            e.printStackTrace();
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

//            rst = new SchedulePeriodicTimeout( (int)(3000 * Math.random()) + MsConfig.INDEX_EXCHANGE_PERIOD, MsConfig.INDEX_EXCHANGE_PERIOD);
            rst = new SchedulePeriodicTimeout( MsConfig.INDEX_EXCHANGE_PERIOD, MsConfig.INDEX_EXCHANGE_PERIOD);
            rst.setTimeoutEvent(new TimeoutCollection.EntryExchangeRound(rst));
            trigger(rst, timerPort);

//            rst = new SchedulePeriodicTimeout((int)(4000 * Math.random()) + MsConfig.CONTROL_MESSAGE_EXCHANGE_PERIOD, MsConfig.CONTROL_MESSAGE_EXCHANGE_PERIOD);
            rst = new SchedulePeriodicTimeout(MsConfig.CONTROL_MESSAGE_EXCHANGE_PERIOD, MsConfig.CONTROL_MESSAGE_EXCHANGE_PERIOD);
            rst.setTimeoutEvent(new TimeoutCollection.ControlMessageExchangeRound(rst));
            trigger(rst, timerPort);
        }
    };

    /**
     * Initialize the Index Caches, from the indexes stored in files.
     *
     * @param luceneAdaptor IndexEntryLuceneAdaptor for access to lucene instance.
     * @throws se.sics.ms.common.LuceneAdaptorException
     */
    public void initializeIndexCaches(IndexEntryLuceneAdaptor luceneAdaptor) throws LuceneAdaptorException {

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



    /**
     * Add index entries for the simulator.
     */
    final Handler<AddIndexSimulated> handleAddIndexSimulated = new Handler<AddIndexSimulated>() {
        @Override
        public void handle(AddIndexSimulated event) {
            addEntryGlobal(event.getEntry());
        }
    };

    final Handler<SimulationEventsPort.SearchSimulated.Request> handleSearchSimulated = new Handler<SimulationEventsPort.SearchSimulated.Request>() {
        @Override
        public void handle(SimulationEventsPort.SearchSimulated.Request event) {

            PaginateInfo defaultPaginateInfo = new PaginateInfo(0, 5);
            searchProtocolTracker.initiateShardSearch(event.getSearchPattern(), defaultPaginateInfo, event.getSearchTimeout(), event.getSearchParallelism());
//            startSearch(event.getSearchPattern(), event.getSearchTimeout(), event.getSearchParallelism()); // Update it to get the params from the simulator.
        }
    };

    /**
     * Add a new {@link se.sics.ms.types.IndexEntry} to the system and schedule a timeout
     * to wait for the acknowledgment.
     *
     * @param entry the {@link se.sics.ms.types.IndexEntry} to be added
     */
    private void addEntryGlobal(IndexEntry entry) {

        ScheduleTimeout rst = new ScheduleTimeout(config.getAddTimeout());
        rst.setTimeoutEvent(new AddIndexTimeout(rst, config.getRetryCount(), entry));
        addEntryGlobal(entry, rst);
    }

    /**
     * Add a new {@link se.sics.ms.types.IndexEntry} to the system, add the given timeout to the timer.
     *
     * @param entry   the {@link se.sics.ms.types.IndexEntry} to be added
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

    ClassMatchedHandler<AddIndexEntry.Request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, AddIndexEntry.Request>> handleAddIndexEntryRequest =
            new ClassMatchedHandler<AddIndexEntry.Request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, AddIndexEntry.Request>>() {

                @Override
                public void handle(AddIndexEntry.Request request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, AddIndexEntry.Request> event) {

                    logger.warn("{}: Received add index entry request from : {}", self.getId(), event.getSource());
                    if (!leader || partitionInProgress) {
                        logger.warn("{}: Received request to add entry but self state doesn't permit to move ahead. Returning ... ");
                        return;
                    }

                    initiateEntryAdditionMechanism(request, event.getSource());
                }
            };


    /**
     * Based on the information passed initiate an index entry addition protocol.
     * This mechanism will be used by the
     *
     * @param request Add Entry Request
     * @param source  Source
     */
    private void initiateEntryAdditionMechanism (AddIndexEntry.Request request, DecoratedAddress source) {

        if (!entryAdditionTracker.canTrack()) {
            logger.warn("{}: Unable to track a new entry addition as limit reached !! ", prefix);
            return;
        }

        if (recentRequests.containsKey(request.getEntryAdditionRound())) {
            logger.warn("{}: Seen the request already.", prefix);
            return;
        }

        // FIX ME: Do not add landing entries multiple times.
        if (!markerEntryAdded && !request.getEntry().equals(IndexEntry.DEFAULT_ENTRY)) {
            logger.warn("{}: Unable to start the addition of index entries as the landing entry has not been added yet.", prefix);
            return;
        }

        if (leaderGroupInformation != null && !leaderGroupInformation.isEmpty()) {

            logger.debug("{} :Reached at the stage of starting with the entry commit ... ", prefix);
            recentRequests.put(request.getEntryAdditionRound(), System.currentTimeMillis());
            IndexEntry newEntry = request.getEntry();

            EntryAddPrepare.Request addPrepareRequest;
            ApplicationEntry applicationEntry;
            LeaderUnit lastEpochUpdate = null;

            if (newEntry.equals(IndexEntry.DEFAULT_ENTRY)) {

                logger.debug(" {}: Going to add a new landing entry in the system. ", prefix);
                lastEpochUpdate = landingEntryTracker.getPreviousEpochContainer() != null 
                        ? landingEntryTracker.getPreviousEpochContainer() 
                        : null;
                
                applicationEntry = new ApplicationEntry(new ApplicationEntry.ApplicationEntryId( 
                        landingEntryTracker.getEpochId(), self.getId(), ApplicationConst.LANDING_ENTRY_ID), newEntry );
                
                addPrepareRequest = new LandingEntryAddPrepare.Request( request.getEntryAdditionRound(), 
                        applicationEntry, lastEpochUpdate );

            } else {

                logger.debug("{}: Enriching the application entry with necessary data ", prefix);
                long id = getNextInsertionId();
                newEntry.setId(id);
                newEntry.setLeaderId(publicKey);
                String signature = ApplicationSecurity.generateSignedHash(newEntry, privateKey);

                if (signature == null) {
                    logger.warn("Unable to generate the hash for the index entry with id: {}", newEntry.getId());
                    return;
                }

                newEntry.setHash(signature);
                applicationEntry = new ApplicationEntry(new ApplicationEntry.ApplicationEntryId(currentEpoch, self.getId(), id), newEntry);
                addPrepareRequest = new ApplicationEntryAddPrepare.Request(request.getEntryAdditionRound(), applicationEntry);
            }


            EntryAdditionRoundInfo additionRoundInfo = new EntryAdditionRoundInfo( request.getEntryAdditionRound(), 
                    leaderGroupInformation, applicationEntry, 
                    source, lastEpochUpdate);
            
            entryAdditionTracker.startTracking(request.getEntryAdditionRound(), additionRoundInfo);
            logger.debug("Started tracking for the entry addition with id: {} for address: {}", newEntry.getId(), source);

            for (DecoratedAddress destination : leaderGroupInformation) {
                logger.debug("Sending prepare commit request to : {}", destination.getId());
                trigger( CommonHelper.getDecoratedContentMessage(self.getAddress(), destination, Transport.UDP, addPrepareRequest), networkPort );
            }

            ScheduleTimeout st = new ScheduleTimeout(5000);
            st.setTimeoutEvent(new TimeoutCollection.EntryPrepareResponseTimeout(st, request.getEntryAdditionRound()));
            entryPrepareTimeoutMap.put(request.getEntryAdditionRound(), st.getTimeoutEvent().getTimeoutId());
            trigger(st, timerPort);

        } else {
            logger.warn("{}: Unable to start the index entry commit due to insufficient information about leader group.", prefix);
        }

    }


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


    /**
     * Get the current insertion id and
     * increment it to keep track of the next one.
     *
     * @return a new id for a new {@link se.sics.ms.types.IndexEntry}
     */
    private long getNextInsertionId() {
        
        if (nextInsertionId == Long.MAX_VALUE - 1)
            nextInsertionId = Long.MIN_VALUE;

        return nextInsertionId ++;
    }

    /**
     * No acknowledgment for an issued {@link se.sics.ms.messages.AddIndexEntryMessage.Request} was received
     * in time. Try to add the entry again or responds with failure to the web client.
     */
    final Handler<AddIndexTimeout> handleAddRequestTimeout = new Handler<AddIndexTimeout>() {
        @Override
        public void handle(AddIndexTimeout event) {

            timeStoringMap.remove(event.getTimeoutId());

            if (event.reachedRetryLimit()) {
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
     * Handler for the Prepare Request Phase of the two phase index entry add commit. The node needs to check for the landing entry
     * and make necessary modifications in the structure used to hold the associated data.
     */
    ClassMatchedHandler<ApplicationEntryAddPrepare.Request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, ApplicationEntryAddPrepare.Request>> handleEntryAddPrepareRequest =
            new ClassMatchedHandler<ApplicationEntryAddPrepare.Request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, ApplicationEntryAddPrepare.Request>>() {

                @Override
                public void handle(ApplicationEntryAddPrepare.Request request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, ApplicationEntryAddPrepare.Request> event) {

                    logger.debug("{}: Received Application Entry addition prepare request from the node: {}", self.getId(), event.getSource());
                    handleEntryAddPrepare(request, event.getSource());
                }
            };


    /**
     * Handler for the Prepare Request Phase of the two phase index entry add commit. The node needs to check for the landing entry
     * and make necessary modifications in the structure used to hold the associated data.
     */
    ClassMatchedHandler<LandingEntryAddPrepare.Request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, LandingEntryAddPrepare.Request>> handleLandingEntryAddPrepareRequest =
            new ClassMatchedHandler<LandingEntryAddPrepare.Request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, LandingEntryAddPrepare.Request>>() {

                @Override
                public void handle(LandingEntryAddPrepare.Request request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, LandingEntryAddPrepare.Request> event) {

                    logger.debug("{}: Received Landing Entry prepare request from the node: {}", self.getId(), event.getSource());
                    handleEntryAddPrepare(request, event.getSource());
                }
            };


    /**
     * Handle the entry addition prepare message from the leader in the partition.
     * Promise needs to be made in case the leader verification is complete.
     *
     * @param request EntryPrepare Request
     * @param source  Message Source.
     */
    private void handleEntryAddPrepare(EntryAddPrepare.Request request, DecoratedAddress source) {


        ApplicationEntry applicationEntry = request.getApplicationEntry();
        IndexEntry entry = applicationEntry.getEntry();
        
        // FIX : Fix the below condition as it seems tricky.
        if (!entry.equals(IndexEntry.DEFAULT_ENTRY) && (!ApplicationSecurity.isIndexEntrySignatureValid(entry) || !leaderIds.contains(entry.getLeaderId()))) {
            logger.warn("{}: Received a promise for entry addition from unknown node: {}", prefix, source);
            return;
        }

        EntryAddPrepare.Response response;
        LeaderUnit previousEpochUpdate = null;

        ApplicationEntry.ApplicationEntryId entryId = new ApplicationEntry.ApplicationEntryId(
                applicationEntry.getEpochId(),
                applicationEntry.getLeaderId(),
                applicationEntry.getEntryId());

        if (entry.equals(IndexEntry.DEFAULT_ENTRY)) {

            logger.debug("{}: Promising for landing entry with details : {}", prefix, applicationEntry.getApplicationEntryId());
            LandingEntryAddPrepare.Request landingEntryRequest = (LandingEntryAddPrepare.Request) request;
            previousEpochUpdate = landingEntryRequest.getPreviousEpochUpdate();

        }

        response = new ApplicationEntryAddPrepare.Response(request.getEntryAdditionRound(), entryId);
        trigger(CommonHelper.getDecoratedContentMessage(self.getAddress(), source, Transport.UDP, response), networkPort);

        ScheduleTimeout st = new ScheduleTimeout(config.getReplicationTimeout());
        st.setTimeoutEvent(new AwaitingForCommitTimeout(st, request.getApplicationEntry()));
        st.getTimeoutEvent().getTimeoutId();

        pendingForCommit.put( applicationEntry, org.javatuples.Pair.with(st.getTimeoutEvent().getTimeoutId(), previousEpochUpdate) );
        trigger(st, timerPort);
    }


    /**
     * The promise for the index entry addition expired and therefore the entry needs to be removed from the map.
     */
    final Handler<AwaitingForCommitTimeout> handleAwaitingForCommitTimeout = new Handler<AwaitingForCommitTimeout>() {
        @Override
        public void handle(AwaitingForCommitTimeout awaitingForCommitTimeout) {

            logger.warn("{}: Index entry prepare phase timed out. Reset the map information.");
            if (pendingForCommit.containsKey(awaitingForCommitTimeout.getApplicationEntry()))
                pendingForCommit.remove(awaitingForCommitTimeout.getApplicationEntry());
        }
    };


    /**
     * Prepare Commit Message from the peers in the system. Update the tracker and check if all the nodes have replied and
     * then send the commit message request to the leader nodes who have replied yes.
     */
    ClassMatchedHandler<ApplicationEntryAddPrepare.Response, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, ApplicationEntryAddPrepare.Response>> handleEntryAdditionPrepareResponse =
            new ClassMatchedHandler<ApplicationEntryAddPrepare.Response, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, ApplicationEntryAddPrepare.Response>>() {


                @Override
                public void handle(ApplicationEntryAddPrepare.Response response, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, ApplicationEntryAddPrepare.Response> event) {

                    logger.debug("{}: Received Index entry prepare response from:{}", self.getId(), event.getSource());

                    UUID entryAdditionRoundId = response.getEntryAdditionRound();
                    EntryAdditionRoundInfo info = entryAdditionTracker.getEntryAdditionRoundInfo(entryAdditionRoundId);

                    if (info == null) {
                        logger.debug("{}: Received Promise Response from: {} after the round has expired ", self.getId(), event.getSource());
                        return;
                    }

                    if(info.isPromiseMajority()){
                        logger.warn("{}: Majority already achieved", prefix);
                        return;
                    }

                    info.addEntryAddPromiseResponse(response);

                    if (info.isPromiseMajority()) {

                        try {

                            logger.debug("{}: Majority nodes have promised for entry addition. Move to commit. ", self.getId());

                            CancelTimeout ct = new CancelTimeout(entryPrepareTimeoutMap.get(entryAdditionRoundId));
                            trigger(ct, timerPort);

                            ApplicationEntry entryToCommit = info.getApplicationEntry();
                            UUID commitTimeout = UUID.randomUUID(); //What's it purpose.

                            if (entryToCommit.getEntry().equals(IndexEntry.DEFAULT_ENTRY)) {

                                logger.debug("{}: Request to add a new landing entry in system", prefix);

                                // Encapsulate the below structure in a separate method.
                                LeaderUnit update = new BaseLeaderUnit(entryToCommit.getEpochId(), entryToCommit.getLeaderId());
                                addUnitPacket(info.getAssociatedEpochUpdate(), update);

                                lowestMissingEntryTracker.updateInternalState(); // Update the internal state of the Missing Tracker.
                                self.resetContainerEntries(); // Update the epoch container entries to be 0, in case epoch gets added.
                                nextInsertionId = ApplicationConst.STARTING_ENTRY_ID; // Reset the insertion id for the current container.

                            } else {
                                logger.debug(" {}: Reached at stage of committing actual entries:{} in the system.", prefix, entryToCommit);
                                pushEntry(entryToCommit);   // Commit to local first.
                            }
                            

                            ByteBuffer idBuffer = ByteBuffer.allocate((8 * 2) + 4);
                            idBuffer.putLong(entryToCommit.getEpochId());
                            idBuffer.putInt(entryToCommit.getLeaderId());
                            idBuffer.putLong(entryToCommit.getEntryId());

                            String signature = ApplicationSecurity.generateRSASignature(idBuffer.array(), privateKey);
                            EntryAddCommit.Request entryCommitRequest = new EntryAddCommit.Request( commitTimeout, new ApplicationEntry.ApplicationEntryId( entryToCommit.getEpochId(),
                                    entryToCommit.getLeaderId(),
                                    entryToCommit.getEntryId()),
                                    signature );

                            for (DecoratedAddress destination : info.getLeaderGroupAddress()) {
                                trigger(CommonHelper.getDecoratedContentMessage(self.getAddress(), destination, Transport.UDP, entryCommitRequest), networkPort);
                            }

                            // Send reply to the originator node. ( Not actually two phase commit as I assume that they will have added entries. )
                            AddIndexEntry.Response addEntryResponse = new AddIndexEntry.Response(info.getEntryAdditionRoundId());
                            trigger(CommonHelper.getDecoratedContentMessage(self.getAddress(), info.getEntryAddSourceNode(), Transport.UDP, addEntryResponse), networkPort);

                        } catch (Exception e) {
                            e.printStackTrace();
                            throw new RuntimeException("Entry addition failed", e);
                        } finally {

                            entryAdditionTracker.resetTracker(entryAdditionRoundId);
                            entryPrepareTimeoutMap.remove(entryAdditionRoundId);
                        }
                    }
                }
            };


    /**
     * Method which needs to be invoked in case the 
     * leader pushes the entry onto the leader group in the system. 
     *  
     * This method addition increments the max leader unit entries which can be then fetched
     * by the lower nodes in the system.
     *
     * @param entry Application Entry.
     */
    private void pushEntry(ApplicationEntry entry) throws IOException, LuceneAdaptorException {

        // Check for the leader unit and update the max entries
        // in the leader unit.
        updateLeaderUnitEntries(entry);
        
        // Once the leader unit is updated with the number of entries,
        // try to add entries in the system.
        logger.warn("{}: Call to push Entry in the system, Entry: {}", self.getId(), entry);
        commitEntryLocally(entry);
        
    }
    
    
    
    
    
    /**
     * Method enclosing the semantics associated 
     * with the process of adding the entries in the system.
     * 
     * @param entry entry
     * @throws IOException
     * @throws LuceneAdaptorException
     */
    private void commitEntryLocally( ApplicationEntry entry ) throws IOException, LuceneAdaptorException {


        if ( lowestMissingEntryTracker.isNextEntryToAdd(entry.getApplicationEntryId())) {
            
            commitAndUpdateUtility(entry);
            lowestMissingEntryTracker.updateTrackerInfo(entry);
            lowestMissingEntryTracker.checkAndRemoveEntryGaps();        
            lowestMissingEntryTracker.printCurrentTrackingInfo();

            // Check for the maximum entries in the leader unit.
            if (self.getEpochContainerEntries() >= config.getMaxEpochContainerSize() && leader) {

                logger.warn("{}: Time to initiate the container switch.", prefix);
                addMarkerUnit();
                return;
            }

            // If container switch is not going on, check for the sharding update.
            checkAndInitiateSharding();
        }
        
        else {
            lowestMissingEntryTracker.bufferEntry(entry);
        }
        
    }
    
    
    
    /**
     * The application entry is added as part of the 
     * push protocol. Update the entries in the leader unit 
     * 
     * Better semantics of the 
     * @param entry entry
     */
    private void updateLeaderUnitEntries( ApplicationEntry entry ){
        
        LeaderUnit unit = timeLine.getLooseUnit(entry.getEpochId(), entry.getLeaderId());
        if(unit == null){
            throw new RuntimeException(" Buffered units not handled yet. ");
        }
        
        long existingEntries = unit.getNumEntries();
        ApplicationEntry.ApplicationEntryId entryId = entry.getApplicationEntryId();

        // As the entry number start from 0.
        // This is required in case a node suddenly becomes a part of leader group and doesn't have the 
        // previous entries.
        
        long resultantEntries = existingEntries >= (entryId.getEntryId() + 1) ? existingEntries 
                : (entryId.getEntryId() + 1); 
        
        unit.setNumEntries(resultantEntries);
    }

    /**
     * Usually the unit commit happens in a pair,
     * the leader closes previous update and then
     * commit the current update. It may be a shard or a simple 
     * unit switch or a network partition commit.
     *
     */
    private void addUnitPacket(LeaderUnit... units){

        for (LeaderUnit unit : units) {

            if(unit == null)
                continue;

            if(timeLine.isSafeToAdd(unit))
            {
                if(!addUnitAndCheckSafety(unit)){
                    break;  // Stop adding beyond unsafe.
                }
            }
            
            else bufferedUnits.add(unit);
        }
    }
    
    
    
    
    /**
     * Wrapper method to perform the addition of the leader unit
     * to the Time Line.
     *
     * @param leaderUnit LeaderUnit.
     */
    private boolean addUnitAndCheckSafety(LeaderUnit leaderUnit){

        boolean result = true;
        LeaderUnit storedUnit = timeLine.getLooseUnit(leaderUnit);
        
        if(storedUnit != null 
                && storedUnit.getLeaderUnitStatus() == LeaderUnit.LUStatus.COMPLETED){
            
            // Check for the update. It might happen that the unit trying to 
            // add is already present and closed. ( Usually happens in ShardUpdates and NPUpdates
            // which are already added as self contained closed units. )
            
            logger.debug("{}: Unit already completed, returning .. ", prefix);      // An Important Check, as Sharding Might Start Happening Again.
            return true;
        }
        
        if (timeLine.isSafeToAdd(leaderUnit) )
        {

            if (leaderUnit instanceof ShardLeaderUnit) {

                // Don' t handle any more update after the shard update.
                logger.debug("{}: Handling Shard Leader Unit.", prefix);
                
                handleSharding((ShardLeaderUnit) leaderUnit);
                gradientEntrySet.clear();    // Clear the gradient entry set to prevent pulling the next partition data from other shard nodes.
                bufferedUnits.clear();      // Expire any buffered units as they might be misleading at this point.
                
                result = false;
            }

            else if (leaderUnit instanceof NPLeaderUnit) {
                // Don't handle any updates after the NP Leader Unit for now. 
                // We can handle updates after it also.
                logger.debug("{}: Handling NP Leader Unit.", prefix);
                result = false;
            }
            
            else{
                logger.debug("{}: Basic Leader Unit Update", prefix);
                addUnitToTimeLine(leaderUnit);
            }

        }
        
        else{
            // Buffering needs to go here.
            throw new IllegalStateException(" Not safe to add entry in the system. Should have been checked earlier. ");
        }
        
        return result;
    }


    /**
     * In case the leader unit that was added by the leader
     * is not in order regarding the current last leader unit,
     * the unit is buffered.
     * 
     * NOTE: It might be that the buffered list
     * already contains the leader unit, therefore check before adding.
     * 
     * @param leaderUnit unit toPENDING buffer
     */
    private void bufferUnit(LeaderUnit leaderUnit) {
       
        int index = -1;
        for(int i=0, len = bufferedUnits.size() ; i < len ; i++){
            
            if(bufferedUnits.get(i).getEpochId() == leaderUnit.getEpochId()
                    && bufferedUnits.get(i).getLeaderId() == leaderUnit.getLeaderId()){
                index= i;
                break;
            }
        }
        
        if(index != -1){
            bufferedUnits.set(index, leaderUnit);
        }
        else {
            bufferedUnits.add(leaderUnit);
            Collections.sort(bufferedUnits, luComparator);
        }
    }

    /**
     * Simply add leader unit to the time line.
     * At this point the in order addition needs to be implemented by the application.
     * Because the time line adds whatever is given to it to add.
     * The application needs to check for the in order add themselves.
     * 
     * @param unit uni to add.
     */
    private void addUnitToTimeLine(LeaderUnit unit) {

        try {

            // MAIN MARKER ENTRY INJECTION POINT.
            if(timeLine.getLooseUnit (unit) == null) {

                MarkerEntry markerEntry = new MarkerEntry(unit.getEpochId(),
                        unit.getLeaderId());

                Document d = MarkerEntry.MarkerEntryHelper.createDocumentFromEntry(markerEntry);
                markerEntryLuceneAdaptor.addDocumentToLucene(d);
                self.incrementEntries();
            }

            timeLine.addLeaderUnit(unit);
            self.setLastLeaderUnit(timeLine.getLastUnit());

            //Some might become applicable to be added.
            informListeningComponentsAboutUpdates(self);
        } 
        catch (LuceneAdaptorException e) {
            
            e.printStackTrace();
            throw new RuntimeException("Unable to add marker entry in system",e);
        }
    }
    

    /**
     * When a leader unit is added in the time line,
     * it might be possible that a buffered unit becomes available 
     * for application.
     */
    private void checkBufferedUnit(){

        Collections.sort(bufferedUnits, luComparator);
        Iterator<LeaderUnit> unitIterator = bufferedUnits.iterator();
        
        while(unitIterator.hasNext()){

            LeaderUnit nextUnit = unitIterator.next();
            if(timeLine.isSafeToAdd(nextUnit))
            {
                boolean nextSafety = addUnitAndCheckSafety(nextUnit);
                unitIterator.remove();
                
                if(!nextSafety) { // stop adding any more buffered units if they are not safe to add.
                    break;
                }
            }
            
            else break; // Don't wait for next as they are sorted and therefore break now.
        }
    }
    
    /**
     * Handler for the entry commit request as part of the index entry addition protocol.
     * Verify that the request is from the leader and then add the entry to the node.
     * <p/>
     * <b>CAUTION :</b> Currently we are not replying to the node back and simply without any questioning add the entry
     * locally, simply the verifying the signature.
     */
    ClassMatchedHandler<EntryAddCommit.Request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, EntryAddCommit.Request>> handleEntryCommitRequest =
            new ClassMatchedHandler<EntryAddCommit.Request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, EntryAddCommit.Request>>() {

                @Override
                public void handle(EntryAddCommit.Request request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, EntryAddCommit.Request> event) {

                    logger.debug("{}: Received index entry commit request from : {}", self.getId(), event.getSource());
                    ApplicationEntry.ApplicationEntryId applicationEntryId = request.getEntryId();

                    if (leaderIds.isEmpty()) {
                        logger.error(" {}: Leader Id's is empty returning ", self.getId());
                        return;
                    }

                    ByteBuffer idBuffer = ByteBuffer.allocate((2 * 8) + 4);
                    idBuffer.putLong(applicationEntryId.getEpochId());
                    idBuffer.putInt(applicationEntryId.getLeaderId());
                    idBuffer.putLong(applicationEntryId.getEntryId());

                    try {
                        if (!ApplicationSecurity.verifyRSASignature( idBuffer.array(), leaderIds.get(leaderIds.size() - 1), request.getSignature())){
                            logger.warn("{}: Returning as signature not verified ... ", self.getId());
                            return;
                        }

                    } catch(Exception e){
                        e.printStackTrace();
                        throw new RuntimeException("Entry commit failed", e);
                    }

                    ApplicationEntry toCommit = null;
                    for (ApplicationEntry appEntry : pendingForCommit.keySet()) {
                        if (appEntry.getApplicationEntryId().equals(applicationEntryId)) {
                            toCommit = appEntry;
                            break;
                        }
                    }

                    if (toCommit == null) {
                        logger.warn("{}: Unable to find application entry to commit.", self.getId());
                        return;
                    }

                    CancelTimeout ct = new CancelTimeout(pendingForCommit.get(toCommit).getValue0());
                    trigger(ct, timerPort);

                    try {

                        LeaderUnit associatedUnitUpdate = pendingForCommit.get(toCommit).getValue1();
                        if (toCommit.getEntry().equals(IndexEntry.DEFAULT_ENTRY)) {

                            logger.warn("{}: Request to add a new landing entry in system", prefix);
                            LeaderUnit update = new BaseLeaderUnit(toCommit.getEpochId(), toCommit.getLeaderId());
                            addUnitPacket(associatedUnitUpdate, update);

                            // As you are directly updating the epoch history,
                            // missing tracker needs to be informed about it.
                            lowestMissingEntryTracker.updateInternalState();

                        }
                        else {
                            // Landing Entry is no longer stored with Index Entries.
                            pushEntry(toCommit);
                        }

                    } 
                    catch (Exception e) {
                        throw new RuntimeException("Unable to process Entry Commit Request, exiting ... ");
                    }
                    
                    finally {
                        pendingForCommit.remove(toCommit); // Remove Entry to prevent memory leak.
                    }

                }
            };


    /**
     * Handler for the add index entry response message in the system.
     * The response is sent by the leader.
     */
    ClassMatchedHandler<AddIndexEntry.Response, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, AddIndexEntry.Response>> handleAddIndexEntryResponse =
            new ClassMatchedHandler<AddIndexEntry.Response, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, AddIndexEntry.Response>>() {

                @Override
                public void handle(AddIndexEntry.Response response, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, AddIndexEntry.Response> event) {

                    logger.debug("{}: Received add index entry response back:", self.getId());
                    CancelTimeout ct = new CancelTimeout(response.getEntryAdditionRound());
                    trigger(ct, timerPort);

                    checkForLandingEntryAdd(response.getEntryAdditionRound());

                    timeStoringMap.remove(response.getEntryAdditionRound());
                    trigger(new UiAddIndexEntryResponse(true), uiPort);

                }
            };


    /**
     * In special case in which the entry might be a landing entry, check based on the round id and
     * reset the landing entry addition meta data and update the current epochId with which the leader will be adding the entry in the system.
     */
    private void checkForLandingEntryAdd(UUID roundId) {

        logger.debug("{}: Checking for response for landing entry addition", prefix);
        if (landingEntryTracker.getLandingEntryRoundId() != null && roundId.equals(landingEntryTracker.getLandingEntryRoundId())) {

            currentEpoch = landingEntryTracker.getEpochId();
            markerEntryAdded = true;
            landingEntryTracker.resetTracker();
        }
    }


    /**
     * Periodically garbage collect the data structure used to identify
     * duplicated {@link se.sics.ms.messages.AddIndexEntryMessage.Request}.
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
            startSearch(searchRequest.getPattern(), null, null);
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
            
            searchPartitionsNumber.put(numberOfPartitions.getTimeoutId(), 
                    numberOfPartitions.getNumberOfShards());
            
            searchRequestStarted.put(numberOfPartitions.getTimeoutId(), new Pair<Long, Integer>(System.currentTimeMillis(),
                    numberOfPartitions.getNumberOfShards()));
        }
    };

    /**
     * Send a search request for a given search pattern to one node in each
     * partition except the local partition.
     *
     * @param pattern the search pattern
     */
    private void startSearch(SearchPattern pattern, Integer searchTimeout, Integer fanoutParameter) {

        // TO DO: Add check for the same request but a different page ( Implement Pagination ).
        searchRequest = new LocalSearchRequest(pattern);
        closeIndex(searchIndex);

        searchIndex = new RAMDirectory();
        searchEntryLuceneAdaptor = new ApplicationLuceneAdaptorImpl(searchIndex, indexWriterConfig);

        try {
            searchEntryLuceneAdaptor.initialEmptyWriterCommit();
        } catch (LuceneAdaptorException e) {
            e.printStackTrace();
            throw new RuntimeException("Unable to open search index", e);
        }

        logger.error("Search Timeout from Application: {}", searchTimeout);
        ScheduleTimeout rst = new ScheduleTimeout(searchTimeout != null ? searchTimeout : config.getQueryTimeout());
        rst.setTimeoutEvent(new TimeoutCollection.SearchTimeout(rst));
        searchRequest.setSearchRoundId(rst.getTimeoutEvent().getTimeoutId());

        trigger(rst, timerPort);
        trigger(new GradientRoutingPort.SearchRequest( pattern, searchRequest.getSearchRoundId(),
                config.getQueryTimeout(), fanoutParameter), gradientRoutingPort);
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
     * Handler for the search request received. The search request contains query to be searched in the local write lucene index.
     */
    ClassMatchedHandler<SearchInfo.Request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, SearchInfo.Request>> handleSearchRequest =
            new ClassMatchedHandler<SearchInfo.Request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, SearchInfo.Request>>() {

                @Override
                public void handle(SearchInfo.Request request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, SearchInfo.Request> event) {

                    logger.debug("{}: Received Search Request from : {}", self.getId(), event.getSource());
                    try {

                        ArrayList<ApplicationEntry> result = searchLocal( writeEntryLuceneAdaptor,
                                request.getPattern(), config.getHitsPerQuery());
                        
                        SearchInfo.ResponseUpdated searchMessageResponse = new SearchInfo.ResponseUpdated( request.getRequestId(),
                                result, request.getPartitionId(), 0, 0);
                        
                        trigger(CommonHelper.getDecoratedContentMessage( self.getAddress(), event.getSource(),
                                Transport.UDP, searchMessageResponse), networkPort );
                        
                    } catch (LuceneAdaptorException e) {
                        logger.warn("{} : Unable to search for index entries in Lucene.", self.getId());
                        e.printStackTrace();
                    }

                }
            };


    // ======================================== CHANGES FOR THE PAGINATION FIX


    /**
     * Send a search request for a given search pattern to one node in each
     * shard except the local partition.
     *
     * @param pattern the search pattern
     */
    private void initiateSearch(SearchPattern pattern, Integer searchTimeout, Integer fanoutParameter) {

        // TO DO: Add check for the same request but a different page ( Implement Pagination ).
        searchRequest = new LocalSearchRequest(pattern);
        closeIndex(searchIndex);

        searchIndex = new RAMDirectory();
        searchEntryLuceneAdaptor = new ApplicationLuceneAdaptorImpl(searchIndex, indexWriterConfig);

        try {
            searchEntryLuceneAdaptor.initialEmptyWriterCommit();
        } catch (LuceneAdaptorException e) {
            e.printStackTrace();
            throw new RuntimeException("Unable to open search index", e);
        }

        logger.error("Search Timeout from Application: {}", searchTimeout);
        ScheduleTimeout rst = new ScheduleTimeout(searchTimeout != null ? searchTimeout : config.getQueryTimeout());
        rst.setTimeoutEvent(new TimeoutCollection.SearchTimeout(rst));
        searchRequest.setSearchRoundId(rst.getTimeoutEvent().getTimeoutId());

        trigger(rst, timerPort);
        trigger(new GradientRoutingPort.SearchRequest( pattern, searchRequest.getSearchRoundId(),
                config.getQueryTimeout(), fanoutParameter), gradientRoutingPort);
    }
















    /**
     * Based on the query pattern and the limit of the responses, execute the
     * query and construct the collection to be returned sorted on the score of the
     * matching documents.
     *
     * @param adaptor Adaptor
     * @param pattern search pattern
     * @param limit limit for responses.
     *
     * @return Collection.
     * @throws LuceneAdaptorException
     */
    private List<IdScorePair> getIdScoreCollection(ApplicationLuceneAdaptor adaptor, SearchPattern pattern, int limit) throws LuceneAdaptorException {

        TopDocsCollector collector = TopScoreDocCollector.create(limit, true);
        return adaptor.getIdScoreCollection(pattern.getQuery(), collector);
    }







    // ==========================================



    /**
     * Query the given index store with a given search pattern.
     *
     * @param adaptor adaptor to use
     * @param pattern the {@link se.sics.ms.types.SearchPattern} to use
     * @param limit   the maximal amount of entries to return
     * @return a list of matching entries
     * @throws java.io.IOException if Lucene errors occur
     */
    private ArrayList<ApplicationEntry> searchLocal( ApplicationLuceneAdaptor adaptor, SearchPattern pattern, int limit ) throws LuceneAdaptorException {
        
        TopScoreDocCollector collector = TopScoreDocCollector.create(limit, true);
        ArrayList<ApplicationEntry> entryResult = (ArrayList<ApplicationEntry>) adaptor.searchApplicationEntriesInLucene(pattern.getQuery(), collector);
        
        return entryResult != null ? entryResult : new ArrayList<ApplicationEntry>();
    }


    /**
     * Node received search response for the current search request.
     * 
     */
    ClassMatchedHandler<SearchInfo.ResponseUpdated, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, SearchInfo.ResponseUpdated>> handleSearchResponse = 
            new ClassMatchedHandler<SearchInfo.ResponseUpdated, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, SearchInfo.ResponseUpdated>>() {
                
        @Override
        public void handle(SearchInfo.ResponseUpdated response, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, SearchInfo.ResponseUpdated> event) {

            if (searchRequest == null || !response.getSearchTimeoutId().equals(searchRequest.getSearchRoundId())) {
                return;
            }
            addSearchResponse( response.getResults(), 
                    response.getPartitionId(), 
                    response.getSearchTimeoutId());
        }
    };

    /**
     * Add all entries from a {@link se.sics.ms.messages.SearchMessage.Response} to the search index.
     *
     * @param entries   the entries to be added
     * @param partition the partition from which the entries originate from
     */
    private void addSearchResponse (Collection<ApplicationEntry> entries, int partition, UUID requestId) {
        
        if (searchRequest.hasResponded(partition)) {
            return;
        }

        try
        {
            addEntries(searchEntryLuceneAdaptor, entries);
            searchRequest.addRespondedPartition(partition);

            Integer numOfPartitions = searchPartitionsNumber.get(requestId);
            if (numOfPartitions == null) {
                return;
            }

            if (searchRequest.getNumberOfRespondedPartitions() == numOfPartitions) {

                logSearchTimeResults(requestId, System.currentTimeMillis(), numOfPartitions);
                CancelTimeout ct = new CancelTimeout(searchRequest.getSearchRoundId());
                trigger(ct, timerPort);
                answerSearchRequestBase();
            }
        }

        catch (IOException e) {
            logger.warn("{}: Unable to add the Search Response from the nodes in the system.", prefix);
        }
    }

    
    /**
     * Log the time results for the search results.
     *  
     * @param requestId requestId
     * @param timeCompleted time of completion
     * @param numOfPartitions partition number
     */
    private void logSearchTimeResults(UUID requestId, long timeCompleted, Integer numOfPartitions) {
        Pair<Long, Integer> searchIssued = searchRequestStarted.get(requestId);
        if (searchIssued == null)
            return;

        if (!searchIssued.getSecond().equals(numOfPartitions))
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
            answerSearchRequestBase();
        }
    };


    /**
     * Based on the median entry and the boolean check, determine
     * the entry base that needs to be removed and ultimately update the
     * self with the remaining entries.
     *
     * @param middleId middle entry.
     * @param isPartition is partition.
     */
    private void removeEntriesNotFromYourShard(ApplicationEntry.ApplicationEntryId middleId, boolean isPartition) {

        try {
            // Remove Entries from the lowest missing tracker also.
            if (isPartition) {

                ApplicationLuceneQueries.deleteDocumentsWithIdMoreThenMod(
                        writeEntryLuceneAdaptor,
                        middleId);

                lowestMissingEntryTracker.deleteDocumentsWithIdMoreThen(middleId);
            } else {

                ApplicationLuceneQueries.deleteDocumentsWithIdLessThenMod(
                        writeEntryLuceneAdaptor,
                        middleId);

                lowestMissingEntryTracker.deleteDocumentsWithIdLessThen(middleId);
            }

            int entrySize = writeEntryLuceneAdaptor.getApplicationEntrySize();
            int markerEntrySize = markerEntryLuceneAdaptor.getMarkerEntriesSize();
            
            logger.warn("{}: After Sharding,  Marker Entry Size :{}, Application Entry Size :{}", new Object[] {prefix, markerEntrySize, entrySize});
            lowestMissingEntryTracker.printExistingEntries();

            // Re-calculate the size of total and the actual entries in the system.
            // Utility comprised of marker entries and the index entries.
            self.setNumberOfEntries(entrySize + markerEntrySize);
            self.setActualEntries(entrySize);

        } catch (LuceneAdaptorException e) {
            e.printStackTrace();
        }
    }

    /**
     * Add the given {@link se.sics.ms.types.ApplicationEntry} to the given Lucene directory
     *
     * @param searchRequestLuceneAdaptor adaptor
     * @param entries                    a collection of index entries to be added
     * @throws java.io.IOException in case the adding operation failed
     */
    private void addEntries(ApplicationLuceneAdaptor searchRequestLuceneAdaptor, Collection<ApplicationEntry> entries)
            throws IOException {
        try {
            for (ApplicationEntry entry : entries) {
                addEntry(searchRequestLuceneAdaptor, entry);
            }
        } catch (LuceneAdaptorException e) {
            logger.warn("{}: Unable to update search index with additional entries", self.getId());
            e.printStackTrace();
        }
    }

    /**
     * The search responses that are collected, are added
     * in the lucene instance and then when its time to reply back,
     * the application simply searches the created lucene instance
     * and reply back. It helps the application to perform manipulations on the data like
     * sorting, searching.
     */
    private void answerSearchRequestBase() {
        
        ArrayList<ApplicationEntry> result = null;
        try {
            result = searchLocal(searchEntryLuceneAdaptor, searchRequest.getSearchPattern(), config.getMaxSearchResults());
            logger.error("{} found {} entries for {}", new Object[]{self.getId(), result.size(), searchRequest.getSearchPattern()});

        } catch (LuceneAdaptorException e) {
            result = new ArrayList<ApplicationEntry>();  // In case of error set the result set as empty.
            logger.warn("{} : Unable to search for the entries.", self.getId());
            e.printStackTrace();
        } finally {
            searchRequest = null;   // Stop handling more searches.
            trigger(new UiSearchResponse(result), uiPort);
        }
    }

    /**
     * Add the given {@link se.sics.ms.types.ApplicationEntry} to the Lucene index using the given
     * writer.
     *
     * @param adaptor the adaptor used to add the {@link se.sics.ms.types.IndexEntry}
     * @param entry   the {@link se.sics.ms.types.IndexEntry} to be added
     * @throws se.sics.ms.common.LuceneAdaptorException in case the adding operation failed
     */
    private void addEntryToLucene(ApplicationLuceneAdaptor adaptor, ApplicationEntry entry) throws LuceneAdaptorException {

        logger.warn("{}: Going to add entry :{} ", prefix, entry.getApplicationEntryId());
        Document doc = new Document();
        doc = ApplicationEntry.ApplicationEntryHelper.createDocumentFromEntry(doc, entry);
        adaptor.addDocumentToLucene(doc);
    }

    /**
     * Add the given {@link se.sics.ms.types.IndexEntry} to the Lucene index using the given
     * writer.
     *
     * @param adaptor the adaptor used to add the {@link se.sics.ms.types.IndexEntry}
     * @param entry   the {@link se.sics.ms.types.IndexEntry} to be added
     * @throws java.io.IOException in case the adding operation failed
     */
    private void addEntry (ApplicationLuceneAdaptor adaptor, ApplicationEntry entry) throws IOException, LuceneAdaptorException {

        logger.trace("{}: Adding entry in the system: {}", self.getId(), entry.getApplicationEntryId());

        Document doc = new Document();
        doc = ApplicationEntry.ApplicationEntryHelper.createDocumentFromEntry(doc, entry);
        adaptor.addDocumentToLucene(doc);
    }

    /**
     * Once the entry passes all the checks for authenticity and being a correctly tracked entry,
     * the method is invoked, which commits it to Lucene and updates the utility.
     *
     * @param entry entry to add.
     */
    private void commitAndUpdateUtility(ApplicationEntry entry) throws LuceneAdaptorException {

        addEntryToLucene(writeEntryLuceneAdaptor, entry);

        // Increment self utility in terms of entries addition to self.
        self.incrementECEntries();
        self.incrementEntries();
        self.incrementActualEntries();

        informListeningComponentsAboutUpdates(self);
    }

    /**
     * Start with the main sharding procedure.
     * Initiate the sharding process.
     */
    private void checkAndInitiateSharding() throws LuceneAdaptorException {

        if (isTimeToShard()) {

            ApplicationEntry.ApplicationEntryId entryId = ApplicationLuceneQueries.getMedianId(writeEntryLuceneAdaptor);

            if (entryId == null || leaderGroupInformation == null || leaderGroupInformation.isEmpty() || !leader) {
                logger.debug("{}: Missing Parameters to initiate sharding, returning ... ", prefix);
                return;
            }

            logger.error("{}: Sharding Median ID: {} ", prefix, entryId);
            partitionInProgress = true;
            
            ScheduleTimeout st1 = new ScheduleTimeout(12000);
            TimeoutCollection.PreShardTimeout preShardTimeout = new TimeoutCollection.PreShardTimeout(st1, entryId);
            st1.setTimeoutEvent(preShardTimeout);
            preShardTimeoutId = st1.getTimeoutEvent().getTimeoutId();
            trigger(st1, timerPort);
            
        } else {
            logger.trace("{}: Not the time to shard, return .. ", prefix);
        }

    }


    /**
     * Pre sharding phase timed out, now lets initiate sharding.
     * The shard protocol gets initiated only after we check that the sharding 
     * condition and the leader condition is still valid after the timeout.
     */
    Handler<TimeoutCollection.PreShardTimeout> preShardTimeoutHandler = new Handler<TimeoutCollection.PreShardTimeout>() {
        @Override
        public void handle(TimeoutCollection.PreShardTimeout event) {

            
            // Some condition check needs to be there.
            
            if (leader && (preShardTimeoutId != null 
                    && preShardTimeoutId.equals(event.getTimeoutId())) ) {
                
                // If after the timeout I am still the leader.
                LeaderUnit previousUpdate = null;
                try {

                    previousUpdate = closePreviousEpoch();

                    ShardLeaderUnit sec = new ShardLeaderUnit(
                            previousUpdate.getEpochId() + 1, self.getId(),
                            ApplicationConst.SHARD_UNIT_SIZE, event.medianId,
                            publicKey );

                    // Create Hash of the Shard Update.
                    String hash = ApplicationSecurity.generateShardSignedHash(sec, privateKey);
                    sec.setHash(hash);

                    ScheduleTimeout st = new ScheduleTimeout(config.getAddTimeout());
                    st.setTimeoutEvent(new TimeoutCollection.ShardRoundTimeout(st, previousUpdate, sec));
                    UUID shardRoundId = st.getTimeoutEvent().getTimeoutId();

                    shardTracker.initiateSharding(shardRoundId, leaderGroupInformation, previousUpdate, sec);
                    trigger(st, timerPort);

                }
                catch (LuceneAdaptorException e) {

                    partitionInProgress = false;
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }    
            }
            
            else {
                logger.debug("{}: Unable to start sharding process as shard conditions don't hold.  ", prefix);
            }
            
            
        }
    };
    
    /**
     * Event from the shard tracker that the sharding round has been completed and therefore
     * the system state needs to be updated in accordance with the sharding.
     *
     * @param shardRoundID shard round
     * @param previousUnit previous unit
     * @param shardUnit    current shard unit
     */
    private void handleSharding(UUID shardRoundID, LeaderUnit previousUnit, LeaderUnit shardUnit) {

        if (shardRoundID != null && !shardTracker.getShardRoundId().equals(shardRoundID)) {
            throw new RuntimeException("Sharding Tracker seems to be corrupted ... ");
        }

        if (shardRoundID != null) {
            CancelTimeout cancelTimeout = new CancelTimeout(shardRoundID);
            trigger(cancelTimeout, timerPort);
            shardTracker.resetShardingParameters();
        }
        
        addUnitPacket(previousUnit, shardUnit);
        partitionInProgress = false;    // What about this resetting of partitioning in progress ?
    }


    /**
     * In case the sharding event is handled by the shard commit or the control pull, the application needs to be informed
     * immediately, so the application can carry out the necessary sharding steps.
     *
     * @param shardUnit Shard Epoch Unit.
     */
    private void handleSharding(ShardLeaderUnit shardUnit) {

        try {

            logger.warn("{}: Handle the main sharding update ... ", prefix);

            ApplicationEntry.ApplicationEntryId medianId = shardUnit.getMedianId();
            lowestMissingEntryTracker.pauseTracking();

            int nodeId = self.getId();
            PartitionId selfPartitionId = new PartitionId(
                    self.getPartitioningType(),
                    self.getPartitioningDepth(),
                    self.getPartitionId());

            boolean partitionSubId = PartitionHelper.determineYourNewPartitionSubId(nodeId, selfPartitionId);
            applyShardingUpdate(partitionSubId, selfPartitionId, medianId);

            lowestMissingEntryTracker.printCurrentTrackingInfo();
            List<LeaderUnit> skipList = generateLUSkipList(shardUnit, medianId, partitionSubId);

            logger.warn("{}: Most Important Part of Sharding generated: {}", prefix, skipList);

            timeLine.addSkipList(skipList);
            addUnitToTimeLine(shardUnit);
            
            logger.debug("{}: TimeLine : {}", prefix, timeLine.getEpochMap());
            lowestMissingEntryTracker.resumeTracking();

        } catch (Exception e) {
            throw new RuntimeException("Unable to shard", e);
        }
    }


    /**
     * Based on the median Id and the current tracking update,
     * generate the skipList which is a list containing the epoch updates
     * that the node has to jump over because the higher nodes might not have the
     * information as they would have removed it as part of there partitioning
     * update.
     *
     * @return Skip List
     */
    private List<LeaderUnit> generateLUSkipList(LeaderUnit shardContainer, ApplicationEntry.ApplicationEntryId medianId, boolean partitionSubId)
            throws IOException, LuceneAdaptorException {

        LeaderUnit lastLeaderUnit = timeLine.getLastUnit();

        if (lastLeaderUnit == null || (lastLeaderUnit.getEpochId() >= shardContainer.getEpochId()
                && lastLeaderUnit.getLeaderId() >= shardContainer.getLeaderId())) {

            throw new IllegalStateException("Sharding State Corrupted ..  " + prefix);
        }

        // Current Tracking might be lagging
        // behind the original information in the store. Therefore Update it before proceeding forward.

        LeaderUnit container = lowestMissingEntryTracker.getCurrentTrackingUnit();
        container = timeLine.getLooseUnit(container);

        if(container == null){
            throw new IllegalStateException("Unable to get updated value for current tracking.. ");
        }

        // Calculate the next in line leader units.
        List<LeaderUnit> pendingUnits = timeLine.getNextLeaderUnits (container);

        // ( In case leader pushes the update to node and it is not in order, just buffer it for the time being. )
        // No updates could be buffered at this point as updates are added in order
        // so the shard update will be the next in line to be added when it was detected.

        pendingUnits.add(container);
        Collections.sort(pendingUnits, luComparator);

        Iterator<LeaderUnit> iterator = pendingUnits.iterator();

        // Based on which section of the entries that the nodes will clear
        // Update the pending list.
        // TO DO : FIX THE ISSUE OF MULTIPLE LEADER ID's IN AN EPOCH for the below fix.

        if (partitionSubId) {
            // If right to the median id is removed, skip list should contain
            // entries to right of the median.
            while (iterator.hasNext()) {

                LeaderUnit nextContainer = iterator.next();
                
                if(nextContainer.getEpochId() == medianId.getEpochId() 
                        && nextContainer.getLeaderId() == medianId.getLeaderId()){
                    break;
                }
                
                if (nextContainer.getEpochId() <= medianId.getEpochId()) {
                    iterator.remove();
                }

            }
        } else {

            // If left to the median is removed, skip list should contain
            // entries to the left of the median.
            while (iterator.hasNext()) {

                LeaderUnit nextContainer = iterator.next();
                
                if( (nextContainer.getEpochId() == medianId.getEpochId() 
                        && nextContainer.getLeaderId() >= medianId.getLeaderId()) || nextContainer.getEpochId() > medianId.getEpochId()){
                    
                    iterator.remove();
                }
            }
        }

        return pendingUnits;
    }


    /**
     * Apply the main sharding update to the application in terms of
     * removing the entries that are not needed and are lying around in the
     * lucene store in the system.
     * <br/>
     * <br/>
     * The order in which the shard updates that needs to be applied is as follows:<br/>
     * <ul>
     * <li>The updates the level and the partitioning information by sharding to
     * to the next level.</li>
     * <p/>
     * <li>The system then removes the entries from the entries that should not lie
     * in the system as part of current partititon information.</li>
     * <p/>
     * <li>The updated self is then pushed to the listening components.
     * </li>
     * </ul>
     *
     * @param medianId Splitting Point.
     */
    public void applyShardingUpdate(boolean partitionSubId, PartitionId selfPartitionId, ApplicationEntry.ApplicationEntryId medianId) {

        shardToNextLevel(
                partitionSubId,
                selfPartitionId);

        removeEntriesNotFromYourShard(
                medianId,
                partitionSubId);

        informListeningComponentsAboutUpdates(self);
    }


    /**
     * Handler for the shard round timeout. Check if the sharding completed and if the sharding expired.
     */
    Handler<TimeoutCollection.ShardRoundTimeout> shardRoundTimeoutHandler = new Handler<TimeoutCollection.ShardRoundTimeout>() {

        @Override
        public void handle(TimeoutCollection.ShardRoundTimeout event) {

            logger.debug("{}: Timeout for shard round invoked.");
            UUID shardTrackerRoundID = shardTracker.getShardRoundId();

            if (shardTrackerRoundID != null && event.getTimeoutId().equals(shardTrackerRoundID)) {
                partitionInProgress = false;
                logger.warn("{}: Need to restart the shard round id");
                throw new UnsupportedOperationException("Operation not supported ... ");
            } else {
                logger.debug("{}: Sharding timeout occured after the event is canceled ... ");
            }


        }
    };


    /**
     * Based on the internal state of the node, check if
     * it's time to shard.
     *
     * @return Shard True/False
     */
    private boolean isTimeToShard() {
        return (self.getActualEntries() >= config.getMaxEntriesOnPeer());
    }

    /**
     * Find an entry for the given id in the local index store.
     *
     * @param entryId the id of the entry
     * @return the entry if found or null if non-existing
     */
    private ApplicationEntry findEntryByIdBase(ApplicationEntry.ApplicationEntryId entryId, TopDocsCollector collector) {
        List<ApplicationEntry> entries = ApplicationLuceneQueries.findEntryIdRange(
                writeEntryLuceneAdaptor,
                entryId, entryId,
                collector);

        if (entries.isEmpty()) {
            return null;
        }
        return entries.get(0);
    }


    /**
     * Find an entry for the given id in the local index store.
     *
     * @param entryId the id of the entry
     * @return the entry if found or null if non-existing
     */
    private ApplicationEntry findEntryById(ApplicationEntry.ApplicationEntryId entryId) {

        ApplicationEntry entry  = ApplicationLuceneQueries.findEntryId(
                writeEntryLuceneAdaptor,
                entryId);

        return entry;
    }


    /**
     * Based on the current shard information, determine the updated shard information
     * value and then split to the current shard to the next level.
     *
     * @return Boolean.
     */
    private boolean shardToNextLevel(boolean partitionSubId, PartitionId selfPartitionId) {

        int nodeId = self.getId();
        int newOverlayId;
        if (selfPartitionId.getPartitioningType() == VodAddress.PartitioningType.NEVER_BEFORE) {

            int partitionId = (partitionSubId ? 1 : 0);

            int selfCategory = self.getCategoryId();
            newOverlayId = OverlayIdHelper.encodePartitionDataAndCategoryIdAsInt(VodAddress.PartitioningType.ONCE_BEFORE,
                    1, partitionId, selfCategory);

        } else {

            int newDepth = self.getPartitioningDepth() + 1;
            int partition = 0;
            for (int i = 0; i < newDepth; i++) {
                partition = partition | (nodeId & (1 << i));
            }

            int selfCategory = self.getCategoryId();

            // Incrementing partitioning depth in the overlayId.
            newOverlayId = OverlayIdHelper.encodePartitionDataAndCategoryIdAsInt(VodAddress.PartitioningType.MANY_BEFORE,
                    newDepth, partition, selfCategory);
        }


        self.setOverlayId(newOverlayId);
        logger.error("Partitioning Occurred at Node: " + self.getId() + " PartitionDepth: " + self.getPartitioningDepth() + " PartitionId: " + self.getPartitionId() + " PartitionType: " + self.getPartitioningType());

        return partitionSubId;
    }


    /**
     * Push updated information to the listening components.
     *
     * @param self Updated Self
     */
    private void informListeningComponentsAboutUpdates(ApplicationSelf self) {

        PeerDescriptor updatedDesc = self.getSelfDescriptor();
        
        selfDescriptor = updatedDesc;
        trigger(new SelfChangedPort.SelfChangedEvent(self), selfChangedPort);
        trigger(new SearchComponentUpdateEvent(new SearchComponentUpdate(updatedDesc, defaultComponentOverlayId)), statusAggregatorPortPositive);
        trigger(new ElectionLeaderUpdateEvent(new ElectionLeaderComponentUpdate(leader, defaultComponentOverlayId)), statusAggregatorPortPositive);
        trigger(new GradientUpdate<PeerDescriptor>(updatedDesc), gradientPort);
        trigger(new ViewUpdate (electionRound, updatedDesc), electionPort);
//        trigger(new PAGUpdate(updatedDesc), pagPort);
    }


    // ======= GRADIENT SAMPLE HANDLER.

    Handler<GradientSample> gradientSampleHandler = new Handler<GradientSample>() {

        @Override
        public void handle(GradientSample event) {

            logger.debug("{}: Received gradient sample", self.getId());
            
            if(selfDescriptor != null 
                    && !selfDescriptor.equals(event.selfView)){
                
                logger.warn("{}: Getting sample for old descriptor from the gradient ... ", prefix);
                return;
            }
            
            gradientEntrySet.clear();

            Collection<Container> collection = event.gradientSample;
            for (Container container : collection) {

                if (container.getContent() instanceof PeerDescriptor) {
                    gradientEntrySet.add((PeerDescriptor) container.getContent());
                }
            }

            publishSample(gradientEntrySet);
        }


    };


    private void publishSample(Set<PeerDescriptor> samples) {

        Set<PeerDescriptor> nodes = samples;
        StringBuilder sb = new StringBuilder("Neighbours: { ");
        for (PeerDescriptor d : nodes) {
            sb.append(d.getVodAddress().getId() + ":" + d.getNumberOfIndexEntries() + ":" + d.getPartitioningDepth() + ":" + d.isLeaderGroupMember()).append(" , ");

        }
        sb.append("}");
        logger.debug(prefix + " " + sb);
    }


    // ************************************
    // LEADER ELECTION PROTOCOL HANDLERS.
    // ************************************

    /**
     * Node is elected as the leader of the partition.
     * In addition to this, node has chosen a leader group which it will work with.
     */
    Handler<LeaderState.ElectedAsLeader> leaderElectionHandler = new Handler<LeaderState.ElectedAsLeader>() {
        @Override
        public void handle(LeaderState.ElectedAsLeader event) {

            try {

                logger.error("{}: Self node is elected as leader.", self.getId());
                leader = true;
                leaderGroupInformation = event.leaderGroup;
                cleanSelfAddress(leaderGroupInformation);
                informListeningComponentsAboutUpdates(self);
                addMarkerUnit();

            } catch (LuceneAdaptorException e) {
                e.printStackTrace();
                throw new RuntimeException("Unable to calculate the Landing Entry on becoming the leader.");
            }


        }
    };


    /**
     * Handler for the leadership extension request in the system.
     *
     */
    Handler<ExtensionUpdate> extensionUpdateHandler  = new Handler<ExtensionUpdate>() {
        @Override
        public void handle(ExtensionUpdate event) {

            logger.warn(" {}: Received Leadership Extension Indication. :{} ", self.getId(), event.groupMembership);
            leaderGroupInformation = event.groupMembership;
            cleanSelfAddress(leaderGroupInformation);
            informListeningComponentsAboutUpdates(self);
        }
    };


    /**
     * Remove the decorated address matching self from the collection.
     * @param collection address collection
     */
    private void cleanSelfAddress(Collection<DecoratedAddress> collection){

        BasicAddress selfPeerAddress = self.getAddress().getBase();
        Iterator<DecoratedAddress> itr = collection.iterator();
        while (itr.hasNext()) {

            BasicAddress addr = itr.next().getBase();
            logger.warn("{}: Leader Group Node :{}", self.getId(), addr.getId());
            if (selfPeerAddress.equals(addr)) {
                itr.remove();
                break;
            }
        }
    }


    /**
     * Once a node gets elected as leader, the parameters regarding the starting entry addition id and the
     * latest epoch id as seen by the node needs to be recalculated and the local parameters need to be updated.
     * Then the leader needs to add landing index entry before anything else.
     */

    private void addMarkerUnit() throws LuceneAdaptorException {

        // Reset the landing entry addition check for the current round of becoming the leader.
        markerEntryAdded = false;

        // Create metadata for the updated epoch update.
        LeaderUnit lastLeaderUnit = closePreviousEpoch();
        long currentEpoch;

        if (lastLeaderUnit == null) {
            logger.warn(" I think I am the first leader in the system. ");
            currentEpoch = ApplicationConst.STARTING_EPOCH;

        } else {
            logger.info("Found the highest known epoch");
            currentEpoch = lastLeaderUnit.getEpochId() + 1;
        }

        ScheduleTimeout st = new ScheduleTimeout(config.getAddTimeout());
        st.setTimeoutEvent(new TimeoutCollection.LandingEntryAddTimeout(st));
        UUID landingEntryRoundId = st.getTimeoutEvent().getTimeoutId();

        landingEntryTracker.startTracking( currentEpoch, landingEntryRoundId, ApplicationConst.LANDING_ENTRY_ID, lastLeaderUnit );
        initiateEntryAdditionMechanism(new AddIndexEntry.Request(landingEntryRoundId, IndexEntry.DEFAULT_ENTRY), self.getAddress());

        logger.warn(landingEntryTracker.toString());
        trigger(st, timerPort);
    }


    /**
     * Check the Lucene Instance for the entries that were added in the
     * precious epoch id instance. The issue with the
     *
     * @return
     * @throws se.sics.ms.common.LuceneAdaptorException
     */
    private LeaderUnit closePreviousEpoch() throws LuceneAdaptorException {

        LeaderUnit lastUnit = timeLine.getLastUnit();

        if (lastUnit != null) {

            if( (lastUnit.getLeaderUnitStatus() != LeaderUnit.LUStatus.COMPLETED)){
                
                Query epochUpdateEntriesQuery = ApplicationLuceneQueries.entriesInLeaderPacketQuery(
                        ApplicationEntry.EPOCH_ID, lastUnit.getEpochId(),
                        ApplicationEntry.LEADER_ID,
                        lastUnit.getLeaderId());

                TotalHitCountCollector hitCollector = new TotalHitCountCollector();
                writeEntryLuceneAdaptor.searchDocumentsInLucene(
                        epochUpdateEntriesQuery,
                        hitCollector);

                int numEntries = hitCollector.getTotalHits();

                lastUnit = new BaseLeaderUnit(
                        lastUnit.getEpochId(),
                        lastUnit.getLeaderId(),
                        numEntries);    
            }
            
            else{
                
                lastUnit = new BaseLeaderUnit(
                        lastUnit.getEpochId(),
                        lastUnit.getLeaderId(),
                        lastUnit.getNumEntries());
            }
            

        }

        return lastUnit;
    }

    /**
     * In case the landing entry was not added in the system.
     */
    Handler<TimeoutCollection.LandingEntryAddTimeout> landingEntryAddTimeout = new Handler<TimeoutCollection.LandingEntryAddTimeout>() {
        @Override
        public void handle(TimeoutCollection.LandingEntryAddTimeout event) {

            logger.warn("{}:Landing Entry Timeout Handler invoked.", prefix);
            if (landingEntryTracker.getLandingEntryRoundId() != null && landingEntryTracker.getLandingEntryRoundId().equals(event.getTimeoutId())) {

                if (leader) {

                    logger.warn(" {}: Landing Entry Commit Failed, so trying again", prefix);

                    ScheduleTimeout st = new ScheduleTimeout(config.getAddTimeout());
                    st.setTimeoutEvent(new TimeoutCollection.LandingEntryAddTimeout(st));
                    UUID landingEntryRoundId = st.getTimeoutEvent().getTimeoutId();

                    // Reset the tracker information for the round.
                    landingEntryTracker.startTracking( landingEntryTracker.getEpochId(), 
                            landingEntryRoundId, ApplicationConst.LANDING_ENTRY_ID, 
                            landingEntryTracker.getPreviousEpochContainer());
                    
                    initiateEntryAdditionMechanism( new AddIndexEntry.Request( landingEntryRoundId, IndexEntry.DEFAULT_ENTRY ), self.getAddress() );

                    trigger(st, timerPort);
                    throw new UnsupportedOperationException(" Operation regarding the restart of the landing entry addition ... ");
                }
            } else {
                logger.warn(" Timeout triggered after landing entry tracker was updated.");
            }
        }
    };

    /**
     * Node was the leader but due to a better node arriving in the system,
     * the leader gives up the leadership in order to maintain fairness.
     */
    Handler<LeaderState.TerminateBeingLeader> terminateBeingLeaderHandler = new Handler<LeaderState.TerminateBeingLeader>() {
        @Override
        public void handle(LeaderState.TerminateBeingLeader event) {
            logger.error("{}: Self is being removed from the leadership position.", self.getId());
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
            leaderAddress = event.leaderAddress;
            leaderKey = event.leaderPublicKey;
        }
    };

    /**
     * Node is chosen by the leader to be part of a leader group. The utility of the node
     * should increase because of this.
     */
    Handler<ElectionState.EnableLGMembership> enableLGMembershipHandler = new Handler<ElectionState.EnableLGMembership>() {
        @Override
        public void handle(ElectionState.EnableLGMembership event) {

            logger.debug("{}: Node is chosen to be a part of leader group.", self.getId());
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

            logger.debug("{}: Remove the node from the leader group membership.", self.getId());
            self.setIsLGMember(false);
            electionRound = event.electionRoundId;
            informListeningComponentsAboutUpdates(self);
        }
    };


    /**
     * Based on the address provided check if the node contains the
     * leader information in the gradient. If leader information found, start a special pull protocol of
     * directly pulling the information from the leader.
     *
     * @param leaderAddress leader address
     * @return true ( if leader present ).
     */
    private boolean isLeaderInGradient(DecoratedAddress leaderAddress) {

        for (PeerDescriptor desc : gradientEntrySet) {
            if (desc.getVodAddress().getBase().equals(leaderAddress.getBase())) {
                return true;
            }
        }

        return false;
    }


    /**
     * Identify the nodes above in the gradient and then return with the higher nodes in the system.
     *
     * @param exchangeNumber exchange number
     * @return Higher Nodes.
     */
    private Collection<DecoratedAddress> getNodesForExchange(int exchangeNumber) {

        Collection<DecoratedAddress> exchangeNodes = new ArrayList<DecoratedAddress>();
        NavigableSet<PeerDescriptor> navigableSet = (NavigableSet<PeerDescriptor>) gradientEntrySet.tailSet(self.getSelfDescriptor());

        Iterator<PeerDescriptor> descendingItr = navigableSet.descendingIterator();

        int counter = 0;
        while (descendingItr.hasNext() && counter < exchangeNumber) {
            exchangeNodes.add(descendingItr.next().getVodAddress());
            counter++;
        }

        return exchangeNodes.size() >= exchangeNumber ? exchangeNodes : null;
    }


    /**
     * Convenient wrapper for canceling a particular
     * timeout.
     *
     * @param timeoutId id for timeout
     */
    private void cancelTimeout(UUID timeoutId){

        if(timeoutId != null ) {

            CancelTimeout ct = new CancelTimeout(timeoutId);
            trigger(ct, timerPort);
        }
    }
    
    /**
     * *****************************
     * PAG Handlers.
     * ***************************** 
     */


    /**
     * Handler for the request to check for presence of 
     * leader unit in the timeline history of the node.
     */
    Handler<LUCheck.Request> leaderUnitCheckHandler = new Handler<LUCheck.Request>() {
        @Override
        public void handle(LUCheck.Request event) {
            
            logger.debug("{}: Received request to look up for a leader unit.");
            boolean result = timeLine.getLooseUnit(event.getEpochId(), event.getLeaderId()) != null;

            LUCheck.Response response = new LUCheck.Response(event.getRequestId(),
                    event.getEpochId(), event.getLeaderId(), result);
            trigger(response, pagPort);
        }
    };


    /**
     * Handler indicating presence of potential network partitioned
     * nodes in the system.
     */
    Handler<NPTimeout> npTimeoutHandler = new Handler<NPTimeout>() {
        @Override
        public void handle(NPTimeout event) {
            
            logger.debug("{}: Received probable partitioned nodes from the PAG");
//            throw new IllegalStateException("Unhandled functionality");
        }
    };


    /**
     * ****************************
     * SEARCH REQUEST HANDLING
     * ****************************
     * <p/>
     * Inner class used to track the different stages / phases as a part of the
     * search request protocol.
     *
     */

    private class SearchProtocolTracker {


        public SearchProtocolTracker (){
            logger.debug("Search Protocol Tracker Booted up.");
        }


        /**
         * Initiate the search protocol by fanning out the search
         * to multiple shards.
         *
         * @param pattern search pattern
         * @param searchTimeout timeout for search
         * @param fanoutParameter search parallelism
         */
        private void initiateShardSearch(SearchPattern pattern, PaginateInfo paginateInfo, Integer searchTimeout, Integer fanoutParameter) {

            ScheduleTimeout rst = new ScheduleTimeout(searchTimeout);
            rst.setTimeoutEvent(new TimeoutCollection.SearchTimeout(rst));

            UUID searchRoundId  = rst.getTimeoutEvent().getTimeoutId();
            searchRequest.startSearch(pattern, paginateInfo, searchRoundId);

            trigger(rst, timerPort);

//          CHECK FOR PATTERN IN CACHE.
            Map<DecoratedAddress, List<IdScorePair>> cachedScoreMap= cache.getScorePairCollection(pattern);

            if(cachedScoreMap != null){

                logger.debug("{}: Started the search with the cached entries.", prefix);
                
                List<IdScorePair> maxHitList = createOrderedMaxHitList(cachedScoreMap);
                Map<DecoratedAddress, List<ApplicationEntry.ApplicationEntryId>> paginateEntryIdMap = prepareFetchPhaseInput(cachedScoreMap,
                        maxHitList, paginateInfo);

                if(paginateEntryIdMap  != null){
                    searchRequest.storeNumHits(maxHitList.size());
                    initiateFetchPhase(paginateEntryIdMap);
                    return;
                }
                
                else{
                    throw new IllegalStateException("Unable to paginate the cached data.");
                }
            }

            logger.error("{}: Going to start default search query phase with pattern:{} ", prefix, searchRequest.getSearchPattern());

            trigger(new GradientRoutingPort.SearchRequest(searchRequest.getSearchPattern(),
                    rst.getTimeoutEvent().getTimeoutId(),
                    searchTimeout, fanoutParameter), gradientRoutingPort);
        }

        /**
         * Handler for information about the number of shards in the system.
         * This information is used by the application to check if all the shards have replied
         * to the search query.
         */
        Handler<NumberOfPartitions> numPartitionsHandler = new Handler<NumberOfPartitions>() {
            @Override
            public void handle(NumberOfPartitions event) {

                logger.debug("{}: Received number of partitions information from the routing component.");
                if(searchRequest.getSearchRoundId() != null
                        && searchRequest.getSearchRoundId().equals(event.getTimeoutId())){

                    // As this message just before the requests are sent out to the
                    // peers in different shards it might happen that the response lags due to large message queue
                    // between the components. Therefore
                    searchRequest.setNumberOfShards(event.getNumberOfShards());
                }
                else {
                    logger.warn("{}: Received information about the partitions for an old request.");
                }
            }
        };

        /**
         * Timeout for the main search protocol phase.
         * The node now looks at the responses that are being collected until this point
         * and then sends the response back.
         *
         */
        Handler<TimeoutCollection.SearchTimeout> searchProtocolTimeout = new Handler<TimeoutCollection.SearchTimeout>() {
            @Override
            public void handle(TimeoutCollection.SearchTimeout event) {

                logger.debug("Search Request Timed out.");
                UUID searchRoundId = searchRequest.getSearchRoundId();

                if(searchRoundId == null || !searchRoundId.equals(event.getTimeoutId())) {
                    logger.warn("{}: Timeout happened after the round already finished.");
                    return;
                }

                searchRequest.wipeExistingRequest();
            }
        };


        /**
         * Handler for the search request received. The search request contains query to be searched in the local write lucene index.
         * The node as part of the query phase simply returns the score and the identifier of the entry matched.
         * The reason behind this process is to help with the pagination.
         *
         */
        ClassMatchedHandler<SearchQuery.Request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, SearchQuery.Request>> handleSearchQueryRequest =

                new ClassMatchedHandler<SearchQuery.Request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, SearchQuery.Request>>() {

            @Override
            public void handle(SearchQuery.Request request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, SearchQuery.Request> event) {

                logger.error("{}: Received Search Query Request from : {}", self.getId(), event.getSource());

                try {
                    List<IdScorePair> idScoreCollection = getIdScoreCollection(writeEntryLuceneAdaptor,
                            request.getPattern(), config.getHitsPerQuery());

                    SearchQuery.Response queryResponse = new SearchQuery.Response(request.getRequestId(), request.getPartitionId(), idScoreCollection);
                    trigger(CommonHelper.getDecoratedContentMessage(self.getAddress(), event.getSource(), Transport.UDP, queryResponse), networkPort);

                } catch (LuceneAdaptorException e) {

                    logger.warn("{} : Unable to query phase of the search request", self.getId());
                    e.printStackTrace();
                }
            }


        };


        /**
         * Handler for the search query response from the nodes in different shards.
         * The node simply checks for the partition id from which the response is received and rejects
         * if already received.
         * <br/>
         * The initiating node waits for the responses selcted nodes from all shards.
         *
         */
        ClassMatchedHandler<SearchQuery.Response, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, SearchQuery.Response>> handleSearchQueryResponse=
                new ClassMatchedHandler<SearchQuery.Response, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, SearchQuery.Response>>() {

                    @Override
                    public void handle(SearchQuery.Response content, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, SearchQuery.Response> event) {

                        logger.error("{}: Received Search Query Response from :{}", self.getId(), event.getSource());
                        UUID searchRoundId = searchRequest.getSearchRoundId();

                        if(searchRoundId == null || !searchRoundId.equals(content.getSearchTimeoutId())) {
                            logger.warn("{}: Received a search query response for an expired round");
                            return;
                        }

                        logger.error("{}: Before safety check ....", prefix);

                        if( searchRequest.isSafeToAdd(content.getPartitionId())) {

                            searchRequest.storeIdScoreCollection(event.getSource(), content.getIdScorePairCollection());
                            if(searchRequest.haveAllShardsResponded()) {

//                               Once all the shards have responded with the information about the matched ids.
                                logger.error("{}: Query phase over, moving to the fetch phase.", self.getId());

//                              COMPUTE THE ORDERED LIST CONTAINING MAX POSSIBLE HITS FOR QUERY.
                                Map<DecoratedAddress, List<IdScorePair>> completeScoreMap = searchRequest.getIdScoreMap();
                                List<IdScorePair> maxHitList = createOrderedMaxHitList(completeScoreMap);

//                              STORE THE METADATA AND UPDATE THE CACHE.
                                searchRequest.storeNumHits(maxHitList.size());
                                cacheScoreMetaData(searchRequest.getSearchPattern(), completeScoreMap, maxHitList);

//                              BASED ON THE PAGINATE INFO, CONSTRUCT THE PAGINATE MAP WHICH IS USED FOR FETCH PHASE.
                                PaginateInfo paginateInfo = searchRequest.getPaginateInfo();
                                Map<DecoratedAddress, List<ApplicationEntry.ApplicationEntryId>> paginateEntryIdMap = prepareFetchPhaseInput(completeScoreMap,
                                        maxHitList, paginateInfo);

                                if(paginateEntryIdMap == null || paginateEntryIdMap.isEmpty()){
                                    logger.warn("{}: Unable to initiate the fetch phase as meta data for the phase not available.", prefix);
                                    return;
                                }

                                initiateFetchPhase(paginateEntryIdMap);
                            }
                        }
                    }
                };



        /**
         * Based on the responses of the query phase, create
         * a deterministic order on the entries. In addition to this,
         * bound the number of entries.
         *
         * @param baseMap baseMap
         * @return filteredList
         */
        private List<IdScorePair> createOrderedMaxHitList( Map<DecoratedAddress, List<IdScorePair>> baseMap){

            List<IdScorePair> scorePairList = new ArrayList<IdScorePair>();
            for(Collection<IdScorePair> collection : baseMap.values()){
                scorePairList.addAll(collection);
            }

//          SORT THE LIST TO CREATE DETERMINISTIC ORDER.
            Collections.sort(scorePairList);

//          LIMIT FINAL SIZE TO MAX SEARCHABLE SIZE.
            if(scorePairList.size() > MsConfig.MAX_SEARCH_ENTRIES) {
                scorePairList = scorePairList.subList(0, MsConfig.MAX_SEARCH_ENTRIES);
            }

            return scorePairList;
        }



        /**
         * Filter and convert the paginate id score pair map to the structure which is required by
         * the fetch phase of the search protocol.
         *
         * @param maxHitList sorted list
         * @param baseMap base score id map.
         * @return fetch phase data structure.
         */
        private Map<DecoratedAddress, List<ApplicationEntry.ApplicationEntryId>> prepareFetchPhaseInput ( Map<DecoratedAddress,List<IdScorePair>> baseMap, List<IdScorePair> maxHitList, PaginateInfo paginateInfo){

            Map<DecoratedAddress, List<ApplicationEntry.ApplicationEntryId>> result = new HashMap<DecoratedAddress, List<ApplicationEntry.ApplicationEntryId>>();

            int from  = paginateInfo.getFrom();
            int size = paginateInfo.getSize() > 0 ? paginateInfo.getSize() : MsConfig.DEFAULT_ENTRIES_PER_PAGE;

            if(from > maxHitList.size()) {

//              AS from IDENTIFIER GREATER THAN SIZE, RETURN.
                logger.warn("{}: Unable to search as range not lying in current range.", prefix);
                return null;
            }

            int to = from + (size);
            to = to > maxHitList.size() ? maxHitList.size() : to;

            List<IdScorePair> paginateList = maxHitList.subList (from, to);
            Map<DecoratedAddress, List<IdScorePair>> paginateScoreMap = createRetainedMap(baseMap, paginateList);

            for(Map.Entry<DecoratedAddress, List<IdScorePair>> entry : paginateScoreMap.entrySet()){

                List<ApplicationEntry.ApplicationEntryId> entryIds = new ArrayList();
                for(IdScorePair pair : entry.getValue()){
                    entryIds.add(pair.getEntryId());
                }
                result.put(entry.getKey(), entryIds);
            }

            return result;
        }

        /**
         * Before the initiation of the fetch phase,
         * the data pulled during the query phase needs to be cached, to be used on subsequent
         * requests.
         */
        private void cacheScoreMetaData(SearchPattern pattern, Map<DecoratedAddress, List<IdScorePair>> baseMap, List<IdScorePair> baseList){

            Map<DecoratedAddress, List<IdScorePair>> retainedMap = createRetainedMap(baseMap, baseList);
            cache.cachePattern(pattern, retainedMap);

            ScheduleTimeout st = new ScheduleTimeout(MsConfig.SCORE_DATA_CACHE_TIMEOUT);
            TimeoutCollection.CacheTimeout ct = new TimeoutCollection.CacheTimeout(st, pattern);
            st.setTimeoutEvent(ct);

            trigger(st, timerPort);
        }


        /**
         * Application needs to remove the cached search request as
         * more entries could have been added in the mean time and therefore
         * in order to make them searchable, older cached ones needs to be deleted.
         */
        Handler<TimeoutCollection.CacheTimeout> cacheTimeoutHandler = new Handler<TimeoutCollection.CacheTimeout>() {
            @Override
            public void handle(TimeoutCollection.CacheTimeout event) {

                logger.debug("{}: Cache timeout handler invoked for file pattern: {}", prefix, event.fileNamePattern);
                cache.removeCachedPattern(event.fileNamePattern);
            }
        };


        /**
         * Helper method to create a retained collection from the
         * values supplied.
         *
         * @param baseMap baseMap
         * @param referenceList referenceList
         * @return  collection
         */
        private Map<DecoratedAddress, List<IdScorePair>> createRetainedMap (Map<DecoratedAddress, List<IdScorePair>> baseMap, List<IdScorePair> referenceList){

            Map<DecoratedAddress, List<IdScorePair>> result = new HashMap<DecoratedAddress, List<IdScorePair>>();

            for(Map.Entry<DecoratedAddress, List<IdScorePair>> entry : baseMap.entrySet()) {
                List<IdScorePair> retainedValue = new ArrayList<IdScorePair>(entry.getValue());

                retainedValue.retainAll(referenceList);
                if(!retainedValue.isEmpty()){
                    result.put(entry.getKey(), retainedValue);
                }
            }

            return result;
        }


        /**
         * Based on the sorted identifiers and the pagination information,
         * initiate the fetch phase for the application entries.
         */
        private void initiateFetchPhase(Map<DecoratedAddress, List<ApplicationEntry.ApplicationEntryId>> fetchPhaseInput) {


            for(Map.Entry<DecoratedAddress, List<ApplicationEntry.ApplicationEntryId>> entry : fetchPhaseInput.entrySet()) {

                SearchFetch.Request fetchRequest = new SearchFetch.Request( searchRequest.getSearchRoundId(), entry.getValue() );
                trigger(CommonHelper.getDecoratedContentMessage(self.getAddress(), entry.getKey(),
                        Transport.UDP, fetchRequest), networkPort);
            }

//          Inform the tracker about the fetch phase.
            searchRequest.initiateFetchPhase(fetchPhaseInput);
        }


        /**
         * Handler for the search fetch request generated by the node once
         * receives meta information concerning the entries that matched the query.
         */
        ClassMatchedHandler<SearchFetch.Request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, SearchFetch.Request>> handleSearchFetchRequest =
                new ClassMatchedHandler<SearchFetch.Request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, SearchFetch.Request>>() {

            @Override
            public void handle(SearchFetch.Request content, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, SearchFetch.Request> event) {

                try {
                    logger.debug("{}: Received search fetch request.", prefix);
                    Collection<ApplicationEntry.ApplicationEntryId> entryIds = content.getEntryIds();
                    List<ApplicationEntry> entries = writeEntryLuceneAdaptor.getApplicationEntries(entryIds);

                    SearchFetch.Response response = new SearchFetch.Response(content.getFetchRequestId(), entries);
                    trigger(CommonHelper.getDecoratedContentMessage(self.getAddress(), event.getSource(), Transport.UDP, response), networkPort);

                } catch (LuceneAdaptorException e) {
                    e.printStackTrace();
                    logger.error("{}: Unable to fetch the entries from the data store during the fetch phase.");
                }
            }
        };


        /**
         * Handler for the search fetch response which involves handling of the responses containing the
         * application entries.
         */
        ClassMatchedHandler<SearchFetch.Response, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, SearchFetch.Response>> handleSearchFetchResponse =
                new ClassMatchedHandler<SearchFetch.Response, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, SearchFetch.Response>>() {
            @Override
            public void handle(SearchFetch.Response content, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, SearchFetch.Response> event) {

                logger.debug("{}: Received search fetch response from :{}", prefix, event.getSource());
                UUID searchRoundId = searchRequest.getSearchRoundId();

                if(searchRoundId == null || !searchRoundId.equals(content.getFetchRequestId())) {
                    logger.warn("{}: Received Search Fetch Respnse for an expired response.");
                }

//              MAIN HANDLING OF RESPONSE.
                searchRequest.addFetchPhaseResponse(event.getSource(), content.getApplicationEntries());
                if(searchRequest.isSafeToRespond()) {

//                  PACK DATA TOGETHER AND SEND BACK RESPONSE.
                    sendResponse();

//                  CANCEL TIMEOUT AND CLEAR ROUND INFORMATION.
                    cancelTimeout(searchRequest.getSearchRoundId());
                    searchRequest.wipeExistingRequest();
                }
            }
        };


        /**
         * Wrapper over the main response dispatch method.
         */
        private void sendResponse(){

            List<ApplicationEntry> entries = searchRequest.getFetchedEntries();
            int numHits = searchRequest.getNumHits();
            SearchPattern pattern = searchRequest.getSearchPattern();

            if(entries == null){
                entries = new ArrayList<ApplicationEntry>();
            }

            sendResponse(numHits, searchRequest.getPaginateInfo(),  pattern, entries);
        }


        /**
         * Once the application entries for the request have been identified, then
         * the response needs to be sent back to the requesting client.
         *
         * @param entries
         */
        private void sendResponse(int numHits, PaginateInfo paginateInfo, SearchPattern pattern,  List<ApplicationEntry> entries){

            SearchResponse response = new SearchResponse(entries, numHits, paginateInfo, pattern);

            logger.debug("{}: Search Response generated . {}", prefix , response);
            trigger(response, uiPort);
        }

    }

    /**
     * ********************************
     * SHARDING PROTOCOL TRACKER
     * ********************************
     * <p/>
     * Main tracker for the sharding protocol,
     * in which the leader informs the leader group nodes about the
     * shard being overgrown in size which needs to be partitioned.
     */

    public class ShardTracker {
        
        private UUID shardRoundId;
        private LeaderUnitUpdate epochUpdatePacket;
        private Collection<DecoratedAddress> cohorts;
        private int promises = 0;
        private org.javatuples.Pair<UUID, LeaderUnitUpdate> shardPacketPair;
        private UUID awaitShardCommit;

        public ShardTracker() {
            logger.debug("{}: Shard Tracker Initialized ", prefix);
        }

        /**
         * Start the sharding protocol. The protocol simply performs a 2 phase
         * commit indicating the nearby nodes of event of sharding in which the nodes based on the
         * based on the state choose a side and remove the entries to balance out the load.
         *
         * @param roundId roundId
         */
        public void initiateSharding(UUID roundId, Collection<DecoratedAddress> leaderGroupInformation, LeaderUnit previousContainer, LeaderUnit shardContainer) {

            if (this.shardRoundId != null || leaderGroupInformation == null || leaderGroupInformation.isEmpty()) {
                logger.warn("{}: Conditions to initiate sharding not satisfied, returning ... ", prefix);
                return;
            }

            shardRoundId = roundId;
            cohorts = leaderGroupInformation;
            epochUpdatePacket = new LeaderUnitUpdate(previousContainer, shardContainer);

            ShardingPrepare.Request request = new ShardingPrepare.Request(shardRoundId,
                    epochUpdatePacket,
                    new OverlayId(self.getOverlayId()));

            for (DecoratedAddress destination : cohorts) {
                trigger(CommonHelper.getDecoratedContentMessage(self.getAddress(), destination, Transport.UDP, request), networkPort);
            }
        }


        public void resetShardingParameters() {

            this.shardRoundId = null;
            this.cohorts = null;
            this.promises = 0;
            this.shardPacketPair = null;
        }

        public UUID getShardRoundId() {
            return this.shardRoundId;
        }


        ClassMatchedHandler<ShardingPrepare.Request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, ShardingPrepare.Request>> shardingPrepareRequest =
                new ClassMatchedHandler<ShardingPrepare.Request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, ShardingPrepare.Request>>() {

                    @Override
                    public void handle(ShardingPrepare.Request request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, ShardingPrepare.Request> event) {

                        logger.debug("{}: Sharding Prepare request received from : {} ", prefix, event.getSource());

                        LeaderUnitUpdate eup = request.getEpochUpdatePacket();
                        ShardLeaderUnit sec = null;

                        if (!(eup.getCurrentEpochUpdate() instanceof ShardLeaderUnit)) {
                            throw new RuntimeException("Unable to proceed with sharding as no shard update found.");
                        }

                        sec = (ShardLeaderUnit) eup.getCurrentEpochUpdate();
                        // Verify the hash update, if verified, then move to commit.

                        if (!ApplicationSecurity.isShardUpdateValid(sec)) {
                            logger.warn("{}: Unable to verify the hash of the update received, returning ... ");
                            return;
                        }

                        shardPacketPair = org.javatuples.Pair.with(request.getShardRoundId(), request.getEpochUpdatePacket());
                        ShardingPrepare.Response response = new ShardingPrepare.Response(request.getShardRoundId());

                        ScheduleTimeout st = new ScheduleTimeout(3000);
                        st.setTimeoutEvent(new TimeoutCollection.AwaitingShardCommit(st));
                        awaitShardCommit = st.getTimeoutEvent().getTimeoutId();

                        trigger(CommonHelper.getDecoratedContentMessage(self.getAddress(), event.getSource(), Transport.UDP, response), networkPort);
                        trigger(st, timerPort);

                    }
                };


        Handler<TimeoutCollection.AwaitingShardCommit> awaitingShardCommitHandler = new Handler<TimeoutCollection.AwaitingShardCommit>() {
            @Override
            public void handle(TimeoutCollection.AwaitingShardCommit event) {

                logger.debug("{}: Awaiting for the Shard Commit. ", prefix);

                if (awaitShardCommit != null && awaitShardCommit.equals(event.getTimeoutId())) {
                    shardPacketPair = null;
                } else {
                    logger.debug("{}: Timeout triggered after being canceled.");
                }
            }
        };


        /**
         * Handle the shard responses from the node in the system.
         */
        ClassMatchedHandler<ShardingPrepare.Response, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, ShardingPrepare.Response>> shardingPrepareResponse =
                new ClassMatchedHandler<ShardingPrepare.Response, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, ShardingPrepare.Response>>() {

                    @Override
                    public void handle(ShardingPrepare.Response response, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, ShardingPrepare.Response> event) {

                        logger.debug("{}: Received Sharding Prepare Response from node : {} ", prefix, event.getSource());

                        if (shardRoundId == null || !shardRoundId.equals(response.getShardRoundId())) {
                            logger.warn("{}: Received a sharding response for an expired round, returning ... ", prefix);
                            return;
                        }


                        if (promises >= cohorts.size()) {
                            logger.warn("{}: All the necessary promises have already been received, returning .. ", prefix);
                            return;
                        }

                        promises++;

                        if (promises >= cohorts.size()) {

                            try{

                                logger.warn("{}: Sharding Promise round over, moving to commit phase ", prefix);
                                ShardingCommit.Request request = new ShardingCommit.Request(shardRoundId);

                                for (DecoratedAddress destination : cohorts) {
                                    trigger(CommonHelper.getDecoratedContentMessage(self.getAddress(), destination, Transport.UDP, request), networkPort);
                                }

                                // For now let's apply the partitioning update on the majority of responses.

                                handleSharding( shardRoundId,
                                        epochUpdatePacket.getPreviousEpochUpdate(),
                                        epochUpdatePacket.getCurrentEpochUpdate());

                                ShardLeaderUnit slu = (ShardLeaderUnit)epochUpdatePacket.getCurrentEpochUpdate();

                                ApplicationEntry shardEntry = new ApplicationEntry(
                                        new ApplicationEntry.ApplicationEntryId(slu.getEpochId(), slu.getLeaderId(), 0));
                                
                                commitEntryLocally(shardEntry);
                            } 
                            
                            catch(Exception e){
                                throw new RuntimeException("Unable to complete sharding commit.", e);
                            }
                        }


                    }
                };


        /**
         * Handler for the sharding commit request from the leader in the system.
         */
        ClassMatchedHandler<ShardingCommit.Request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, ShardingCommit.Request>> shardingCommitRequest =
                new ClassMatchedHandler<ShardingCommit.Request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, ShardingCommit.Request>>() {

                    @Override
                    public void handle(ShardingCommit.Request request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, ShardingCommit.Request> event) {

                        logger.debug("{}: Sharding commit request handler invoked ... ", prefix);
                        UUID receivedShardRoundID = request.getShardRoundId();
                        UUID storedShardRoundID = shardPacketPair != null ? shardPacketPair.getValue0() : null;

                        if (storedShardRoundID == null || !storedShardRoundID.equals(receivedShardRoundID)) {
                            logger.warn("{}: Received a request for an expired shard round id, returning ... ");
                            return;
                        }
                        
                        try{

                            // Cancel the awaiting timeout.
                            UUID timeoutId = shardPacketPair.getValue0();
                            CancelTimeout ct = new CancelTimeout(timeoutId);
                            trigger(ct, timerPort);
                            awaitShardCommit = null;

                            // Shard the node.
                            LeaderUnitUpdate updatePacket = shardPacketPair.getValue1();
                            handleSharding( null, updatePacket.getPreviousEpochUpdate(), updatePacket.getCurrentEpochUpdate() );

                            ShardLeaderUnit slu = (ShardLeaderUnit) updatePacket.getCurrentEpochUpdate();
                            ApplicationEntry entry = new ApplicationEntry(
                                    new ApplicationEntry.ApplicationEntryId(slu.getEpochId(), slu.getLeaderId(),0));

                            // Here it might be possible that the missing tracker buffers the entry instead of adding it.
                            // But eventually the tracker should see the entry buffered and add it to lucene.
                            
                            commitEntryLocally(entry);

                            ShardingCommit.Response response = new ShardingCommit.Response(receivedShardRoundID);
                            trigger(CommonHelper.getDecoratedContentMessage(self.getAddress(), event.getSource(), Transport.UDP, response), networkPort);
                            
                        }
                        catch(Exception e){
                            throw new RuntimeException("Unable to complete the sharding process", e);
                        }
                        
                    }
                };


        ClassMatchedHandler<ShardingCommit.Response, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, ShardingCommit.Response>> shardingCommitResponse =
                new ClassMatchedHandler<ShardingCommit.Response, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, ShardingCommit.Response>>() {

                    @Override
                    public void handle(ShardingCommit.Response content, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, ShardingCommit.Response> event) {

                        logger.debug("{}: Received sharding commit response from the node :{}", prefix, event.getSource());
                    }
                };


    }


    /**
     * *********************************
     * CONTROL PULL TRACKER
     * *********************************
     * <p/>
     * Tracker for the main control pull mechanism.
     */
    private class ControlPullTracker {


        private UUID currentPullRound;
        private Map<DecoratedAddress, ControlPull.Response> pullResponseMap;
        private LeaderUnit currentUpdate;

        public ControlPullTracker() {
            pullResponseMap = new HashMap<DecoratedAddress, ControlPull.Response>();
        }
        private GenericECComparator comparator = new GenericECComparator();

        /**
         * Initiate the main control pull mechanism. The mechanism simply asks for any updates that the nodes might have
         * seen as compared to the update that was sent by the requesting node.
         * <p/>
         * The contract for the control pull mechanism is that the mechanism keeps track of the current
         * update that it has information as provided by the history tracker. The request for the next updates are made with respect to the
         * current update. The contract simply states that the replying node should reply with the updated value of the epoch that the node
         * has requested
         */
        Handler<TimeoutCollection.ControlMessageExchangeRound> exchangeRoundHandler =
                new Handler<TimeoutCollection.ControlMessageExchangeRound>() {

                    @Override
                    public void handle(TimeoutCollection.ControlMessageExchangeRound controlMessageExchangeRound) {

                        logger.debug("{}: Initiating the control message exchange round", prefix);

                        Collection<DecoratedAddress> addresses = getNodesForExchange(config.getIndexExchangeRequestNumber());
                        if (addresses == null || addresses.size() < config.getIndexExchangeRequestNumber()) {
                            logger.debug("{}: Unable to start the control pull mechanism as higher nodes are less than required number", prefix);
                            return;
                        }

                        currentPullRound = UUID.randomUUID();
                        currentUpdate = timeLine.getLastUnit();

                        logger.debug("{}: Current Pull Round: {}, Leader Unit: {}", new Object[]{prefix, currentPullRound, currentUpdate});
                        OverlayId overlayId = new OverlayId(self.getOverlayId());

                        ControlPull.Request request = new ControlPull.Request(currentPullRound, overlayId, currentUpdate);
                        pullResponseMap.clear();

                        for (DecoratedAddress destination : addresses) {
                            trigger(CommonHelper.getDecoratedContentMessage(self.getAddress(), destination, Transport.UDP, request), networkPort);
                        }
                    }
                };


        /**
         * Main Handler for the control pull request from the nodes lying low in the gradient. The nodes
         * simply request the higher nodes that if they have seen any information more than the provided information in the
         * request packet and in case information is found, it is encoded generically in a byte array and sent to the
         * requesting nodes in the system.
         */
        ClassMatchedHandler<ControlPull.Request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, ControlPull.Request>> controlPullRequest =
                new ClassMatchedHandler<ControlPull.Request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, ControlPull.Request>>() {

                    @Override
                    public void handle(ControlPull.Request request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, ControlPull.Request> event) {

                        logger.debug("{}: Received Control Pull Request from the node :{} ", prefix, event.getSource());

                        // TO DO: Process the control pull request and then calculate the updates that needs to be sent back to the user.

                        List<LeaderUnit> nextUpdates = new ArrayList<LeaderUnit>();

                        DecoratedAddress address = leaderAddress;
                        PublicKey key = leaderKey;

                        LeaderUnit receivedUnit = request.getLeaderUnit();
                        LeaderUnit updateUnit = (receivedUnit == null) ? timeLine.getInitialTrackingUnit()
                                :timeLine.getSelfUnitUpdate(receivedUnit);

                        if (updateUnit != null) {

                            receivedUnit = updateUnit.shallowCopy();
                            receivedUnit.setEntryPullStatus(LeaderUnit.EntryPullStatus.PENDING);    // Reset the unit status to prevent the node being replied to with wrong status. ( FIXED with SERIALIZATION )

                            nextUpdates.add(receivedUnit);
                            nextUpdates.addAll(timeLine.getNextLeaderUnits(receivedUnit,
                                    config.getMaximumEpochUpdatesPullSize()));
                        }
                        logger.debug("{}: Epoch Update List: {}", prefix, nextUpdates);

                        ControlPull.Response response = new ControlPull.Response(request.getPullRound(), address, key, nextUpdates, self.getOverlayId()); // Handler for the DecoratedAddress
                        trigger(CommonHelper.getDecoratedContentMessage(self.getAddress(), event.getSource(), Transport.UDP, response), networkPort);
                    }
                };


        /**
         * Handler of the control pull response from the nodes that the peer requested control information. The requesting node has
         * the responsibility to actually detect the in order epoch updates and only add those in the system.
         * For now there is no hash mechanism in the control pull mechanism, so we directly get the responses from the
         * nodes, assuming all of them are functioning fine.
         */
        ClassMatchedHandler<ControlPull.Response, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, ControlPull.Response>> controlPullResponse =
                new ClassMatchedHandler<ControlPull.Response, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, ControlPull.Response>>() {

                    @Override
                    public void handle(ControlPull.Response response, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, ControlPull.Response> event) {

                        logger.debug("{}: Received Control Pull Response from the Node: {}", prefix, event.getSource());

                        if (currentPullRound == null || !currentPullRound.equals(response.getPullRound())) {
                            logger.warn("{}: Receiving the Control Pull Response for an expired or unavailable round, returning ...", prefix);
                            return;
                        }

                        if( !PartitionHelper.isOverlayExtension(response.getOverlayId(), self.getOverlayId(), event.getSource().getId()) ){
                            logger.debug("{}: Control Pull response from a node:{} which is not extension of self overlayId:{} ", new Object[]{prefix, event.getSource(), new OverlayId(self.getOverlayId()) });
                            return;
                        }

                        List<LeaderUnit> updates = response.getNextUpdates();

                        if (currentUpdate != null) {

                            if (updates.isEmpty() || !checkOriginalExtension(updates.get(0))) {
                                logger.error("{}: Control exchange protocol violated", new Object[]{prefix});
                                throw new IllegalStateException(" Control Pull Protocol Violated ... ");
                            }
                        }


                        pullResponseMap.put(event.getSource(), response);
                        if (pullResponseMap.size() >= config.getIndexExchangeRequestNumber()) {

                            logger.debug("{}: Pull Response Map: {}", pullResponseMap);
                            performResponseMatch();

                            currentPullRound = null;
                            pullResponseMap.clear();
                        }
                    }
                };


        /**
         * Simply check if the received update is an extension of the original update.
         * There might be a case in which the current tracking update might have been modified.
         *
         * @param receivedUpdate Update Received.
         * @return True is extension of CurrentUpdate.
         */
        private boolean checkOriginalExtension(LeaderUnit receivedUpdate) {
            return (receivedUpdate.getEpochId() == currentUpdate.getEpochId() && receivedUpdate.getLeaderId() == currentUpdate.getLeaderId());
        }


        /**
         * Go through all the responses that the node fetched through the pull mechanism
         * and then find the commonly matched responses and inform the listening components
         * about the updates received.
         */
        private void performResponseMatch() {

            if (pullResponseMap.size() > 0) {

                // Perform the special matching for the leader units.
                matchLeaderUnits();

                // Leader Matching.
                ControlPull.Response baseResponse = pullResponseMap.values()
                        .iterator()
                        .next();

                DecoratedAddress baseLeader = baseResponse.getLeaderAddress();
                PublicKey baseLeaderKey = baseResponse.getLeaderKey();

                if (baseLeader == null || baseLeaderKey == null) {
                    logger.debug("{}: No Common Leader Updates ", prefix);
                    return;
                }

                for (ControlPull.Response response : pullResponseMap.values()) {
                    if (response.getLeaderAddress() == null
                            || !response.getLeaderAddress().equals(baseLeader)
                            || response.getLeaderKey() == null
                            || !response.getLeaderKey().equals(baseLeaderKey)) {

                        logger.debug("{}: Mismatch Found Returning ... ");
                        return;
                    }
                }

                logger.debug("{}: Found the leader update:{}", prefix, baseLeader);

                leaderAddress = baseLeader;
                leaderKey = baseLeaderKey;
                trigger(new LeaderInfoUpdate(baseLeader, baseLeaderKey), leaderStatusPort);     // Inform the gradient about the matched leader update.
            }
        }


        /**
         * Matching of the leader units is tricky and therefore needs to
         * be handled carefully. We need to divide the leader units in two parts,
         * ONGOING and COMPLETED units.
         */
        private void matchLeaderUnits() {

            if( pullResponseMap.size() > 0 ) {

                LeaderUnit ongoingUnit = null;
                List<LeaderUnit> intersection;

                intersection = new ArrayList<LeaderUnit>(pullResponseMap.values().iterator().next().getNextUpdates());

                for(LeaderUnit unit: intersection) {

                    if(unit.getLeaderUnitStatus() == LeaderUnit.LUStatus.ONGOING){

                        // Ideally do not break because there should never be two ongoing units.
                        // Which could be TESTED here. ADD THE CHECK LATER ON.
                        ongoingUnit = unit;
                        break;
                    }
                }

                // Perform the matching of the ongoing leader unit separately.
                if(ongoingUnit != null) {
                    intersection.remove(ongoingUnit);
                }

                // Retain without the ONGOING Value i.e Completed Units.
                // ONGOING One has number of entries and in a very dynamic system it might be that the
                // node discards the update because the value is changing so frequently.

                for (ControlPull.Response response : pullResponseMap.values()) {
                    intersection.retainAll(response.getNextUpdates());
                }

                if (ongoingUnit != null) {

                    long baseEntries= ongoingUnit.getNumEntries();

                    // Check the common ongoing one and take the lowest entries.
                    for(ControlPull.Response response : pullResponseMap.values()){

                        boolean unitPresent = false;
                        for(LeaderUnit unit : response.getNextUpdates()) {

                            if( unit.getEpochId() == ongoingUnit.getEpochId()
                                    && unit.getLeaderId() == ongoingUnit.getLeaderId()) {

                                if(unit.getNumEntries() < baseEntries) {

                                    baseEntries = unit.getNumEntries();
                                    ongoingUnit.setNumEntries(baseEntries);
                                }

                                unitPresent = true;
                                break;
                            }
                        }

                        if(!unitPresent) {
                            ongoingUnit = null;
                            break;
                        }
                    }
                }

                if (ongoingUnit != null) {
                    intersection.add(ongoingUnit);
                }

                Collections.sort(intersection, luComparator);
                addLeaderUnits(intersection);  // Store the common leader units in the system.
            }

        }

        /**
         * Check for the leader units and add them in the timeline.
         * There is a special method used for adding it as the application method is responsible
         * fpor detection on any important in order update.
         *
         * @param units
         */
        private void addLeaderUnits(List<LeaderUnit> units) {

            if (units == null || units.isEmpty()) {
                return;
            }
            
            // Certain housekeeping tasks need to be 
            // performed before the system can move forward in terms
            // of adding new units.
            matchAndRemoveBuffered(units);
            
            // Now add the units to the timeline.
            for (LeaderUnit unit : units) {

                if(timeLine.isSafeToAdd(unit))
                {
                    if(!addUnitAndCheckSafety(unit)){
                        break;  // Stop adding beyond unsafe.
                    }
                    checkBufferedUnit(); // Check buffered units after every addition.
                }
                
                else bufferedUnits.add(unit);
            }
        }


        /**
         * Check common entries between the units received through the pull and the 
         * buffered entries. In case a match is found, remove that buffered entry from the
         * collection.
         * <br/>
         * Assume collection of units to be sorted.
         * @param units units
         */
        private void matchAndRemoveBuffered(List<LeaderUnit> units) {
            
            Collections.sort(bufferedUnits, luComparator);
            Collections.sort(units, luComparator);
            
            Iterator<LeaderUnit> itr = bufferedUnits.iterator();
            
            while(itr.hasNext()) {

                LeaderUnit next = itr.next();
                
                for(LeaderUnit unit : units){
                    
                    if(next.getEpochId() == unit.getEpochId() 
                            && next.getLeaderId() == unit.getLeaderId()){
                        itr.remove();
                    }
                    // In case epoch id has exceeded, break as the units are sorted.
                    if(unit.getEpochId() > next.getEpochId()) {
                        break;
                    }
                }                
            }
            
            // Before returning, check if any buffered entry can be added
            // to the system.
            checkBufferedUnit();
        }
        
        


    }

    /**
     * **********************************
     * LOWEST MISSING ENTRY TRACKER
     * **********************************
     * <p/>
     * Inner class used to keep track of the lowest missing index entry and also
     * communicate with the Epoch History Tracker, which for now keeps history of the
     * epoch updates.
     */
    private class LowestMissingEntryTracker {

        private LeaderUnit currentTrackingUnit;
        private Map<ApplicationEntry.ApplicationEntryId, ApplicationEntry> existingEntries;
        private long currentTrackingId;
        private EntryExchangeTracker entryExchangeTracker;

        private UUID leaderPullRound;   // SPECIAL ID FOR NODES PULLING FROM LEADER.
        private boolean isPaused = false;

        public LeaderUnit getCurrentTrackingUnit() {
            return currentTrackingUnit;
        }

        public LowestMissingEntryTracker() {

            this.entryExchangeTracker = new EntryExchangeTracker(config.getIndexExchangeRequestNumber(), seed);
            this.existingEntries = new HashMap<ApplicationEntry.ApplicationEntryId, ApplicationEntry>();
            this.currentTrackingId = 0;
        }


        public void printCurrentTrackingInfo() throws IOException, LuceneAdaptorException {

            logger.debug("{}: Entry Being Tracked by Application :{} and actual entries: {} and total utility: {}",
                    new Object[]{prefix, getEntryBeingTracked(), self.getActualEntries(), self.getNumberOfEntries()});
        }


        /**
         * Handler for the periodic exchange round handler in the system.
         * The purpose of the exchange round is to initiate the index pull mechanism in the system.
         * The nodes try to catch up as quickly as possible to the leader group nodes and therefore the frequency of the updates should be more.
         */
        public Handler<TimeoutCollection.EntryExchangeRound> entryExchangeRoundHandler = new Handler<TimeoutCollection.EntryExchangeRound>() {
            @Override
            public void handle(TimeoutCollection.EntryExchangeRound entryExchangeRound) {

                try {

                    logger.info("Entry Exchange Round initiated ... ");

                    if (!isPaused) {

                        entryExchangeTracker.resetTracker();
                        updateInternalState();
                        startEntryPullMechanism();

                    } else {
                        logger.error("{}: Entry exchange round is paused, returning ... ");
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                    throw new RuntimeException(" Unable to initiate entry exchange round ", e);
                }

            }
        };


        /**
         * Initiate the main entry pull mechanism.
         */
        private void startEntryPullMechanism() throws IOException, LuceneAdaptorException {

            if (currentTrackingUnit != null) {

                UUID entryExchangeRound = UUID.randomUUID();
                logger.debug(" {}: Starting with the index pull mechanism with exchange round: {} and tracking unit:{} ", new Object[]{ prefix, entryExchangeRound, currentTrackingUnit });
                triggerHashExchange(entryExchangeRound);

            } else {
                logger.debug("{}: Unable to Start Entry Pull as the Insufficient Information about Current Tracking Update", prefix);
            }
        }


        /**
         * Construct the index exchange request and request the higher node
         *
         * @param entryExchangeRound entry exchange round.
         */
        private void triggerHashExchange(UUID entryExchangeRound) throws IOException, LuceneAdaptorException {

            ApplicationEntry.ApplicationEntryId entryBeingTracked = getEntryBeingTracked();
            if (leaderAddress != null && isLeaderInGradient(leaderAddress)) {

                logger.debug("Start the special direct leader pull protocol.");

                leaderPullRound = UUID.randomUUID();
                LeaderPullEntry.Request pullRequest = new LeaderPullEntry.Request(leaderPullRound, entryBeingTracked);
                trigger(CommonHelper.getDecoratedContentMessage(self.getAddress(), leaderAddress, Transport.UDP, pullRequest), networkPort);


            } else {

                EntryHashExchange.Request request =
                        new EntryHashExchange.Request(entryExchangeRound, entryBeingTracked);

                Collection<DecoratedAddress> higherNodesForFetch =
                        getNodesForExchange(entryExchangeTracker.getHigherNodesCount());

                if (higherNodesForFetch == null || higherNodesForFetch.isEmpty()) {
                    logger.info("{}: Unable to start index hash exchange due to insufficient nodes in the system.", prefix);
                    return;
                }

                for (DecoratedAddress destination : higherNodesForFetch) {
                    trigger(CommonHelper.getDecoratedContentMessage(self.getAddress(), destination, Transport.UDP, request), networkPort);
                }
            }

            entryExchangeTracker.startTracking(entryExchangeRound);
        }


        /**
         * Handler for request to pull the entries directly from the leader. The node simply checks
         * the leader information and if the node is currently the leader, then it replies back with the information
         * requested.
         * FIX : This handler is a potential hole for the lower nodes to fetch the entries from the partitioned nodes.
         * So in any handler that deals with returning data to other nodes, the check for the overlay id needs to be there.
         * <b>Every Node</b> only replies with data to the nodes at same level except for the partitioning information.
         */
        ClassMatchedHandler<LeaderPullEntry.Request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, LeaderPullEntry.Request>> leaderPullRequest =
                new ClassMatchedHandler<LeaderPullEntry.Request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, LeaderPullEntry.Request>>() {

                    @Override
                    public void handle(LeaderPullEntry.Request request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, LeaderPullEntry.Request> event) {

                        if (leader) {

                            logger.debug("{}: Direct Leader Pull Request from: {}", prefix, event.getSource());

                            // Return reply if I am leader else chuck it.
                            List<ApplicationEntry> entries = ApplicationLuceneQueries.strictEntryIdRangeOnDefaultSort(writeEntryLuceneAdaptor,
                                    request.getLowestMissingEntryId(), config.getMaxExchangeCount());

                            LeaderPullEntry.Response response = new LeaderPullEntry.Response(request.getDirectPullRound(), entries, self.getOverlayId());
                            trigger(CommonHelper.getDecoratedContentMessage(self.getAddress(), event.getSource(), Transport.UDP, response), networkPort);

                        }

                    }
                };


        /**
         * Main Handler for the pull entry response from the node that the sending node thinks as the current leader.
         * The response if received contains the information of the next predefined entries from the missing entry the
         * system originally asked for.
         */
        ClassMatchedHandler<LeaderPullEntry.Response, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, LeaderPullEntry.Response>> leaderPullResponse =
                new ClassMatchedHandler<LeaderPullEntry.Response, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, LeaderPullEntry.Response>>() {

                    @Override
                    public void handle(LeaderPullEntry.Response response, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, LeaderPullEntry.Response> event) {

                        logger.debug("{}: Received leader pull response from the node: {} in the system", prefix, event.getSource());

                        try {
                            if (leaderPullRound != null && leaderPullRound.equals(response.getDirectPullRound())) {

                                if(!PartitionHelper.isOverlayExtension(response.getOverlayId(), self.getOverlayId(), event.getSource().getId())){
                                    logger.warn("{}: OverlayId extension check failed .. ", prefix);
                                    return;
                                }

                                leaderPullRound = null; // Quickly reset leader pull round to prevent misuse.

                                // Only expected entries i.e. less than limit are allowed.
                                List<ApplicationEntry> entries = new ArrayList<ApplicationEntry>(response.getMissingEntries());
                                Collections.sort(entries, entryComparator);
                                commitPulledEntries(entries);  
                            }

                        } catch (Exception ex) {
                            logger.warn("{}: Entries Pulled : {}", prefix, response.getMissingEntries());
                            throw new RuntimeException("Unable to add entries in System", ex);
                        }
                    }

                };


        /**
         * Handler for the Entry Hash Exchange Request in which the nodes request for the hashes of the next missing index entries
         * as part of the lowest missing entry information.
         */
        ClassMatchedHandler<EntryHashExchange.Request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, EntryHashExchange.Request>> entryHashExchangeRequestHandler =
                new ClassMatchedHandler<EntryHashExchange.Request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, EntryHashExchange.Request>>() {

                    @Override
                    public void handle(EntryHashExchange.Request request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, EntryHashExchange.Request> event) {

                        logger.debug("{}: Received the entry hash exchange request from the node:{} in the system.", prefix, event.getSource());
                        Collection<EntryHash> entryHashs = new ArrayList<EntryHash>();

                        List<ApplicationEntry> applicationEntries = ApplicationLuceneQueries.strictEntryIdRangeOnDefaultSort(
                                writeEntryLuceneAdaptor,
                                request.getLowestMissingIndexEntry(),
                                config.getMaxExchangeCount());

                        for (ApplicationEntry entry : applicationEntries) {
                            entryHashs.add(new EntryHash(entry));
                        }

                        EntryHashExchange.Response response = new EntryHashExchange.Response(request.getExchangeRoundId(), entryHashs);     // Verify the validity.
                        trigger(CommonHelper.getDecoratedContentMessage(self.getAddress(), event.getSource(), Transport.UDP, response), networkPort);
                    }
                };


        /**
         * Handler for the entry hash exchange response in the system. The nodes collect the responses and then
         * analyze the common hashes in order for the nodes to apply.
         */
        ClassMatchedHandler<EntryHashExchange.Response, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, EntryHashExchange.Response>> entryHashExchangeResponseHandler =
                new ClassMatchedHandler<EntryHashExchange.Response, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, EntryHashExchange.Response>>() {

                    @Override
                    public void handle(EntryHashExchange.Response response, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, EntryHashExchange.Response> event) {

                        try {

                            logger.debug("{}: Received hash exchange response for the round: {}", prefix, response.getExchangeRoundId());

                            if (entryExchangeTracker.getExchangeRoundId() == null || !entryExchangeTracker.getExchangeRoundId().equals(response.getExchangeRoundId())) {
                                logger.warn("{}: Received index exchange response for an unknown round :{} , returning ....",
                                        prefix,
                                        response.getExchangeRoundId());
                                return;
                            }

                            entryExchangeTracker.addEntryHashResponse(event.getSource(), response);
                            if (entryExchangeTracker.allHashResponsesComplete()) {

                                Collection<EntryHash> entryHashCollection = entryExchangeTracker.getCommonEntryHashes(entryExchangeTracker
                                        .getExchangeRoundEntryHashCollection()
                                        .values());

                                Collection<ApplicationEntry.ApplicationEntryId> entryIds = new ArrayList<ApplicationEntry.ApplicationEntryId>();

                                if (entryHashCollection.isEmpty()) {
                                    logger.warn("{}: Unable to find any common in order hashes", prefix);
                                    return;
                                }

                                for (EntryHash hash : entryHashCollection) {
                                    entryIds.add(hash.getEntryId());
                                }

                                // Trigger request to get application entries from a particular user.

                                DecoratedAddress destination = entryExchangeTracker.getSoftMaxBasedNode();
                                EntryExchange.Request request = new EntryExchange.Request(entryExchangeTracker.getExchangeRoundId(), entryIds);
                                trigger(CommonHelper.getDecoratedContentMessage(self.getAddress(), destination, Transport.UDP, request), networkPort);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            throw new RuntimeException(" Unable to process Index Hash Exchange Response ", e);
                        }
                    }

                };


        /**
         * In case the common required index hashes are located by the nodes in the system,
         * the node requests for the actual value of the exchange entry from the peer in the system.
         * <p/>
         * The node simply locates for the individual entries required by the node from the lucene instance adds them to the collection
         * to be returned.
         */
        ClassMatchedHandler<EntryExchange.Request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, EntryExchange.Request>> entryExchangeRequestHandler =
                new ClassMatchedHandler<EntryExchange.Request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, EntryExchange.Request>>() {

                    @Override
                    public void handle(EntryExchange.Request request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, EntryExchange.Request> event) {

                        logger.debug("{}: Received Entry Exchange Request", prefix);
                        int defaultLimit = 1;

                        List<ApplicationEntry> applicationEntries = new ArrayList<ApplicationEntry>();
                        TopScoreDocCollector collector = TopScoreDocCollector.create(defaultLimit, true);
                        Collection<ApplicationEntry.ApplicationEntryId> applicationEntryIds = request.getEntryIds();

                        for (ApplicationEntry.ApplicationEntryId entryId : applicationEntryIds) {
                            ApplicationEntry applicationEntry = findEntryById(entryId);
                            if (applicationEntry != null) {
                                applicationEntries.add(applicationEntry);
                            }
                        }

                        EntryExchange.Response response = new EntryExchange.Response(request.getExchangeRoundId(), applicationEntries, self.getOverlayId());
                        trigger(CommonHelper.getDecoratedContentMessage(self.getAddress(), event.getSource(), Transport.UDP, response), networkPort);
                    }
                };


        /**
         * Handler for the actual entries that are received through the last phase of the index pull mechanism.
         * Simply add them to the lowest missing tracker instance, which will itself handle everything from not allowing the wrong
         * or entries out of order to be added in the system.
         */
        ClassMatchedHandler<EntryExchange.Response, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, EntryExchange.Response>> entryExchangeResponseHandler =
                new ClassMatchedHandler<EntryExchange.Response, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, EntryExchange.Response>>() {

                    @Override
                    public void handle(EntryExchange.Response response, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, EntryExchange.Response> event) {

                        logger.debug("{}: Received Entry Exchange Response from :{}", prefix, event.getSource());
                        try {

                            if (entryExchangeTracker.getExchangeRoundId() == null || !entryExchangeTracker.getExchangeRoundId().equals(response.getEntryExchangeRound())) {
                                logger.debug("{}: Received exchange response for the expired round, returning ...", prefix);
                                return;
                            }

                            if(!PartitionHelper.isOverlayExtension(response.getOverlayId(), self.getOverlayId(), event.getSource().getId())){
                                logger.warn("{}: Entry Exchange Response from an undeserving node, returning ... ");
                                return;
                            }

                            List<ApplicationEntry> entries = new ArrayList<ApplicationEntry>(response.getApplicationEntries());
                            Collections.sort(entries, entryComparator);

                            commitPulledEntries(entries);

                        } catch (Exception e) {
                            throw new RuntimeException("Unable to add entries in the Lucene. State Corrupted, exiting ...", e);
                        }
                    }
                };


        /**
         * Do not allow commit entries beyond the specified
         * max entries in the system.
         *  
         * @param entries entry collection
         */
        private void commitPulledEntries (Collection<ApplicationEntry> entries) throws IOException, LuceneAdaptorException {

            Iterator<ApplicationEntry> itr = entries.iterator();
            long limit = currentTrackingUnit.getNumEntries();
            
            if( limit <= 0 )
                return;
            
            while(itr.hasNext()){
                
                long entryId = itr.next().getApplicationEntryId().getEntryId();
                if( entryId >= limit) {
                    // If any entry exceed beyond the limit, remove from the iterator.
                    // Such control is required for the NP Merge.
                    itr.remove();  
                }
            }
                
            // Commit the remaining entries
            for(ApplicationEntry entry: entries) {
                commitEntryLocally(entry);
            }

        }
        
        /**
         * Main helper method for updating the entry tracker which keeps in sync with the
         * timeline about the next tracking id and information.
         */
        public void updateCurrentLUTracking() {

            if(currentTrackingUnit == null){

                // Initial case, we need to fetch the initial tracking unit.
                currentTrackingUnit = timeLine.getInitialTrackingUnit();
                currentTrackingUnit = timeLine.currentTrackUnit(currentTrackingUnit);   // Inform the timeline about the decision to track the unit.
            }

            else {
                currentTrackingUnit = timeLine.getSelfUnitUpdate(currentTrackingUnit);
                if(currentTrackingUnit.getEntryPullStatus() == LeaderUnit.EntryPullStatus.SKIP){
                    checkAndUpdateTracking();
                }

                else if((currentTrackingUnit.getLeaderUnitStatus() == LeaderUnit.LUStatus.COMPLETED)
                        && (currentTrackingId >= currentTrackingUnit.getNumEntries())) {

                    // Application needs to inform the timeline about the completed entry pull status
                    if(currentTrackingUnit.getEntryPullStatus() != LeaderUnit.EntryPullStatus.COMPLETED){
                        currentTrackingUnit = timeLine.markUnitComplete(currentTrackingUnit);
                    }

                    checkAndUpdateTracking();
                }
            }

        }


        /**
         * A simple helper method to check for the
         * next tracking unit and reset the current tracking information
         * if update is found.
         *
         */
        private void checkAndUpdateTracking(){

            LeaderUnit nextUpdate = timeLine
                    .getNextUnitToTrack(currentTrackingUnit);

            if (nextUpdate != null) {

                currentTrackingUnit = timeLine.currentTrackUnit(nextUpdate);
                currentTrackingId = 0;
            }

        }        


        /**
         * Check with the lowest missing tracker about the latest entry to add in the system.
         * In case the entry is not the latest, then the missing tracker will store the entry in a separate map.
         *
         * @param entryId
         * @return Add Entry.
         */
        public boolean isNextEntryToAdd(ApplicationEntry.ApplicationEntryId entryId) throws IOException, LuceneAdaptorException {
            
            return (currentTrackingUnit != null 
                && currentTrackingUnit.getEpochId() == entryId.getEpochId()
                && currentTrackingUnit.getLeaderId() == entryId.getLeaderId()
                && currentTrackingId == entryId.getEntryId());
        }



        /**
         * Application requests to buffer the current entry 
         * in the system. Buffer if only the entry is exactly ahead
         * in terms of entry information.
         *
         * @param entry entry
         *
         */
        public void bufferEntry(ApplicationEntry entry) throws IOException, LuceneAdaptorException {
            
            ApplicationEntry.ApplicationEntryId entryBeingTracked = getEntryBeingTracked();
            
            if( entryBeingTracked.compareTo(entry.getApplicationEntryId()) < 0 ){
                logger.debug("{}:  Buffering the Pulled Entry Locally with current tracking id :{}  ", self.getId(), lowestMissingEntryTracker.getCurrentTrackingUnit());
                existingEntries.put(entry.getApplicationEntryId(), entry);
            }
        }
        
        
        
        /**
         * Once the node receives an entry to be added in the application,
         * the method needs to be invoked, which according to the entry that needs to be added,
         * updates the local existing entries map information.
         * <p/>
         * The methods returns the boolean which informs the application if the entry can be added in the system
         * or not. (It can be added if it is the currently being tracked else goes to the existing entries map).
         *
         * @param entry entry to add.
         * @throws java.io.IOException
         * @throws se.sics.ms.common.LuceneAdaptorException
         */

        public boolean updateTrackerInfo (ApplicationEntry entry) throws IOException, LuceneAdaptorException {

            
            boolean result = false;
            
            if (currentTrackingUnit != null) {

                if ( isNextEntryToAdd(entry.getApplicationEntryId()) ) {

                    logger.info("Received update for the current tracked entry");
                    currentTrackingId++;
                    result = true;
                }
            }
            
            return result;
        }
        

        /**
         * Get the application entry that is being currently tracked by the application.
         * Here Tracking means that application is looking for the entry in the system or in other words waiting for
         * someone or the leader to privide with the entry.
         *
         * @return current entry to pull.
         */
        public ApplicationEntry.ApplicationEntryId getEntryBeingTracked() throws IOException, LuceneAdaptorException {

            ApplicationEntry.ApplicationEntryId entryId = null;

            if (currentTrackingUnit != null) {

                entryId = new ApplicationEntry.ApplicationEntryId(currentTrackingUnit.getEpochId(),
                        currentTrackingUnit.getLeaderId(),
                        currentTrackingId);
            }

            return entryId;
        }

        /**
         * The operation itself does nothing but calls
         * the internal state operations in order.
         */
        public void updateInternalState() throws IOException, LuceneAdaptorException {

            // Update the current tracking information.
            updateCurrentLUTracking();

            // Check for some entry gaps and remove them.
            checkAndRemoveEntryGaps();
        }


        /**
         * Once you add an entry in the system, check for any gaps that might be occurred and can be removed.
         *
         * @throws java.io.IOException
         * @throws se.sics.ms.common.LuceneAdaptorException
         */
        public void checkAndRemoveEntryGaps() throws IOException, LuceneAdaptorException {

            ApplicationEntry.ApplicationEntryId existingEntryId = getEntryBeingTracked();
            while (existingEntries.keySet().contains(existingEntryId)) {

                logger.debug("{}: Picking up entries from the buffered store, {}", self.getId(), existingEntryId);
                commitAndUpdateUtility(existingEntries.get(existingEntryId));
                existingEntries.remove(existingEntryId);
                currentTrackingId++;
                existingEntryId = getEntryBeingTracked();
            }
        }


        // ++++ ====== +++++ MAIN SHARDING APPLICATION +++++ ===== ++++++

        /**
         * As the sharding update is going on which will remove the entries from the timeline,
         */
        private void pauseTracking() {

            logger.debug("{}: Going to pause the entry exchange round .. ", prefix);

            isPaused = true;
            leaderPullRound = null;
            entryExchangeTracker.resetTracker(); // Do not handle any responses at this point.
        }


        /**
         * Once the sharding is over, reset the tracker to point to the
         * next tracking update. Depending upon the status of the current tracker position
         * and the updates to skip, the next tracking information is updated.
         */
        private void resumeTracking() {

            logger.warn("{}: Switching the entry exchange round again ..", prefix);
            isPaused = false;

            // Based on the current tracking information,
            // check if the unit has become obsolete ?

            if (!timeLine.isTrackable(currentTrackingUnit)) {

                currentTrackingUnit = timeLine
                        .getSelfUnitUpdate(currentTrackingUnit);

                LeaderUnit nextUnit = timeLine
                        .getNextUnitToTrack(currentTrackingUnit);

                if (nextUnit != null) {

                    currentTrackingUnit = timeLine.currentTrackUnit(nextUnit);
                    currentTrackingId = 0;
                }
            }

        }


        /**
         * Check for the buffered entries and then remove the entry with
         * id's more than the specified id.
         *
         * @param medianId splitting id.
         */
        private void deleteDocumentsWithIdMoreThen(ApplicationEntry.ApplicationEntryId medianId) {

            Iterator<ApplicationEntry.ApplicationEntryId> idIterator = existingEntries.keySet().iterator();
            while (idIterator.hasNext()) {

                if (idIterator.next().compareTo(medianId) > 0) {
                    idIterator.remove();
                }
            }
        }


        /**
         * Check for the buffered entries and then remove the entry with
         * ids less than the specified id.
         *
         * @param medianId splitting id.
         */
        private void deleteDocumentsWithIdLessThen(ApplicationEntry.ApplicationEntryId medianId) {

            Iterator<ApplicationEntry.ApplicationEntryId> idIterator = existingEntries.keySet().iterator();
            while (idIterator.hasNext()) {

                if (idIterator.next().compareTo(medianId) < 0) {
                    idIterator.remove();
                }
            }
        }


        private void printExistingEntries() {
            logger.warn("Existing Entries {}:", this.existingEntries.toString());
        }

    }
}

