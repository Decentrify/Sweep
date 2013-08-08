package se.sics.ms.search;

import org.apache.commons.codec.binary.Base64;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.*;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.common.Self;
import se.sics.gvod.config.SearchConfiguration;
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
import se.sics.ms.gradient.GradientRoutingPort;
import se.sics.ms.gradient.LeaderStatusPort;
import se.sics.ms.gradient.PublicKeyBroadcast;
import se.sics.ms.peer.IndexPort;
import se.sics.ms.peer.IndexPort.AddIndexSimulated;
import se.sics.ms.gradient.PublicKeyPort;
import se.sics.ms.snapshot.Snapshot;
import se.sics.ms.timeout.IndividualTimeout;
import se.sics.ms.exceptions.IllegalSearchString;
import se.sics.ms.messages.*;
import se.sics.ms.types.IndexEntry;
import se.sics.ms.types.SearchPattern;
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
 */
public final class Search extends ComponentDefinition {
    /**
     * Set to true to store the Lucene index on disk
     */
    public static final boolean PERSISTENT_INDEX = false;

    Positive<IndexPort> indexPort = positive(IndexPort.class);
    Positive<VodNetwork> networkPort = positive(VodNetwork.class);
    Positive<Timer> timerPort = positive(Timer.class);
    Positive<GradientRoutingPort> gradientRoutingPort = positive(GradientRoutingPort.class);
    Negative<LeaderStatusPort> leaderStatusPort = negative(LeaderStatusPort.class);
    Negative<PublicKeyPort> publicKeyPort = negative(PublicKeyPort.class);

    private static final Logger logger = LoggerFactory.getLogger(Search.class);
    private Self self;
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

    private PrivateKey privateKey;
    private PublicKey publicKey;
    private ArrayList<PublicKey> leaderIds = new ArrayList<PublicKey>();
    private HashMap<TimeoutId, IndexEntry> avaitingForPrepairResponse = new HashMap<TimeoutId, IndexEntry>();
    private HashMap<IndexEntry, TimeoutId> pendingForCommit = new HashMap<IndexEntry, TimeoutId>();

    private class ExchangeRound extends IndividualTimeout {

        public ExchangeRound(SchedulePeriodicTimeout request, int id) {
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
         * @param request
         *            the ScheduleTimeout that holds the Timeout
         * @param retryLimit
         *            the number of retries for the related
         *            {@link se.sics.ms.messages.AddIndexEntryMessage.Request}
         * @param entry
         *            the {@link se.sics.ms.types.IndexEntry} this timeout was scheduled for
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
            return numberOfRetries > retryLimit;
        }

        /**
         * @return the {@link IndexEntry} this timeout was scheduled for
         */
        public IndexEntry getEntry() {
            return entry;
        }
    }

    public Search() {
        subscribe(handleInit, control);
        subscribe(handleRound, timerPort);
        subscribe(handleAddIndexSimulated, indexPort);
        subscribe(handleIndexExchangeRequest, networkPort);
        subscribe(handleIndexExchangeResponse, networkPort);
        subscribe(handleAddIndexEntryRequest, networkPort);
        subscribe(handleAddIndexEntryResponse, networkPort);
        subscribe(handleSearchRequest, networkPort);
        subscribe(handleSearchResponse, networkPort);
        subscribe(handleSearchTimeout, timerPort);
        subscribe(handleAddRequestTimeout, timerPort);
        subscribe(handleRecentRequestsGcTimeout, timerPort);
        subscribe(handleLeaderStatus, leaderStatusPort);
        subscribe(handleRepairRequest, networkPort);
        subscribe(handleRepairResponse, networkPort);
        subscribe(handlePublicKeyBroadcast, publicKeyPort);
        subscribe(handlePrepairCommit, networkPort);
        subscribe(handleAwaitingForCommitTimeout, timerPort);
        subscribe(handlePrepairCoomitResponse, networkPort);
        subscribe(handleCommitTimeout, timerPort);
        subscribe(handleCommitRequest, networkPort);
        subscribe(handleCommitResponse, networkPort);
    }

    /**
     * Initialize the component.
     */
    final Handler<SearchInit> handleInit = new Handler<SearchInit>() {
        public void handle(SearchInit init) {
            self = init.getSelf();
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

            if (PERSISTENT_INDEX) {
                File file = new File("resources/index_" + self.getId());
                try {
                    index = FSDirectory.open(file);
                } catch (IOException e1) {
                    // TODO proper exception handling
                    e1.printStackTrace();
                    System.exit(-1);
                }

                if (file.exists()) {
                    try {
                        initializeIndexCaches();
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                        System.exit(-1);
                    }
                }
            } else {
                index = new RAMDirectory();
            }

            recentRequests = new HashMap<TimeoutId, Long>();
            // Garbage collect the data structure
            SchedulePeriodicTimeout rst = new SchedulePeriodicTimeout(
                    config.getRecentRequestsGcInterval(),
                    config.getRecentRequestsGcInterval());
            rst.setTimeoutEvent(new RecentRequestsGcTimeout(rst, self.getId()));
            trigger(rst, timerPort);

            // TODO move time to own config instead of using the gradient period
            rst = new SchedulePeriodicTimeout(MsConfig.GRADIENT_SHUFFLE_PERIOD, MsConfig.GRADIENT_SHUFFLE_PERIOD);
            rst.setTimeoutEvent(new ExchangeRound(rst, self.getId()));
            trigger(rst, timerPort);

            // Can't open the index before committing a writer once
            IndexWriter writer;
            try {
                writer = new IndexWriter(index, indexWriterConfig);
                writer.commit();
                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(-1);
            }
        }
    };

    /**
     * Initialize the local index id cashing data structures from the persistent
     * file.
     *
     * @throws IOException in case errors occur while reading the index
     */
    private void initializeIndexCaches() throws IOException {
        IndexReader reader = null;
        IndexSearcher searcher;
        try {
            reader = DirectoryReader.open(index);
            searcher = new IndexSearcher(reader);

            boolean continuous = true;
            // TODO check which limit performs well
            int readLimit = 20000;
            // Would not terminate in case it reaches the limit of long ;)
            for (long i = 0; ; i += readLimit) {
                Query query = NumericRangeQuery.newLongRange(IndexEntry.ID, i, i + readLimit, true, false);
                TopDocs topDocs = searcher.search(query, readLimit, new Sort(new SortField(IndexEntry.ID, Type.LONG)));

                if (topDocs.totalHits == 0) {
                    break;
                }

                ScoreDoc[] hits = topDocs.scoreDocs;
                if (continuous) {
                    // Get all ids for the next entries
                    Long[] ids = new Long[hits.length];
                    for (int j = 0; j < hits.length; j++) {
                        ids[j] = Long.valueOf(searcher.doc(hits[j].doc).get(IndexEntry.ID));
                    }

                    // Check if there is a gap between the last missing value
                    // and the smallest newly given one
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
                    for (int j = 0; j < hits.length; j++) {
                        existingEntries.add(Long.valueOf(searcher.doc(hits[j].doc).get(IndexEntry.ID)));
                    }
                }
            }
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException ex) {
                }
            }
        }
    }

    /**
     * Issue an index exchange with another node.
     */
    final Handler<ExchangeRound> handleRound = new Handler<ExchangeRound>() {
        @Override
        public void handle(ExchangeRound event) {
            Long[] existing = existingEntries.toArray(new Long[existingEntries.size()]);
            trigger(new GradientRoutingPort.IndexExchangeRequest(lowestMissingIndexValue, existing), gradientRoutingPort);
        }
    };

    /**
     * Search for entries in the local store that the inquirer might need and
     * send them to him.
     */
    final Handler<IndexExchangeMessage.Request> handleIndexExchangeRequest = new Handler<IndexExchangeMessage.Request>() {
        @Override
        public void handle(IndexExchangeMessage.Request event) {
            try {
                List<IndexEntry> indexEntries = new ArrayList<IndexEntry>();

                // Search for entries the inquirer is missing
                Long lastId = event.getOldestMissingIndexValue();
                for (Long i : event.getExistingEntries()) {
                    indexEntries.addAll(findIdRange(lastId, i - 1, config.getMaxExchangeCount() - indexEntries.size()));
                    lastId = i + 1;

                    if (indexEntries.size() >= config.getMaxExchangeCount()) {
                        break;
                    }
                }

                // In case there is some space left search for more
                if (indexEntries.size() < config.getMaxExchangeCount()) {
                    indexEntries.addAll(findIdRange(lastId, Long.MAX_VALUE, config.getMaxExchangeCount() - indexEntries.size()));
                }

                if (indexEntries.isEmpty()) {
                    return;
                }

                trigger(new IndexExchangeMessage.Response(self.getAddress(), event.getVodSource(), event.getTimeoutId(), indexEntries.toArray(new IndexEntry[indexEntries.size()]), 0, 0), networkPort);
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
            try {
                for (IndexEntry indexEntry : event.getIndexEntries()) {
                    addEntryLocal(indexEntry);
                }
            } catch (IOException e) {
                logger.error(self.getId() + " " + e.getMessage());
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
    }

    /**
     * Handler executed in the role of the leader. Create a new id and search
     * for a the according bucket in the routing table. If it does not include
     * enough nodes to satisfy the replication requirements then create a new id
     * and try again. Send a {@link se.sics.ms.messages.ReplicationPrepairCommitMessage} request to a number of nodes as
     * specified in the config file and schedule a timeout to wait for
     * responses. The adding operation will be acknowledged if either all nodes
     * responded to the {@link se.sics.ms.messages.ReplicationPrepairCommitMessage} request or the timeout occurred and
     * enough nodes, as specified in the config, responded.
     */
    final Handler<AddIndexEntryMessage.Request> handleAddIndexEntryRequest = new Handler<AddIndexEntryMessage.Request>() {
        @Override
        public void handle(AddIndexEntryMessage.Request event) {
            if (!leader) {
                return;
            }

            Snapshot.incrementReceivedAddRequests();

            if (recentRequests.containsKey(event.getTimeoutId())) {
                return;
            }
            recentRequests.put(event.getTimeoutId(), System.currentTimeMillis());

            IndexEntry newEntry = event.getEntry();
            long id = getNextInsertionId();
            newEntry.setId(id);
            newEntry.setLeaderId(publicKey);

            String signature =  generateSignedHash(newEntry, privateKey);
            if(signature == null)
                return;

            newEntry.setHash(signature);

            avaitingForPrepairResponse.put(event.getTimeoutId(), newEntry);
            replicationRequests.put(event.getTimeoutId(), new ReplicationCount(event.getVodSource(), config.getReplicationMinimum(), newEntry));

            trigger(new GradientRoutingPort.ReplicationPrepairCommitRequest(newEntry, event.getTimeoutId()), gradientRoutingPort);
        }
    };

    /*
     * @return a new id for a new {@link IndexEntry}
     */
    private long getNextInsertionId() {
        return nextInsertionId++;
    }

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
     * An index entry has been successfully added.
     */
    final Handler<AddIndexEntryMessage.Response> handleAddIndexEntryResponse = new Handler<AddIndexEntryMessage.Response>() {
        @Override
        public void handle(AddIndexEntryMessage.Response event) {
            // TODO inform user
            CancelTimeout ct = new CancelTimeout(event.getTimeoutId());
            trigger(ct, timerPort);
        }
    };

    /**
     * No acknowledgment for an issued {@link AddIndexEntryMessage.Request} was received
     * in time. Try to add the entry again or respons with failure to the web client.
     */
    final Handler<AddIndexTimeout> handleAddRequestTimeout = new Handler<AddIndexTimeout>() {
        @Override
        public void handle(AddIndexTimeout event) {
            if (event.reachedRetryLimit()) {
                Snapshot.incrementFailedddRequests();
                logger.warn("{} reached retry limit for adding a new entry {} ", self.getAddress(), event.entry);
            } else {
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
    final Handler<ReplicationPrepairCommitMessage.Request> handlePrepairCommit = new Handler<ReplicationPrepairCommitMessage.Request>() {
        @Override
        public void handle(ReplicationPrepairCommitMessage.Request request) {
            IndexEntry entry = request.getEntry();
            if(!isIndexEntrySignatureValid(entry) || !leaderIds.contains(entry.getLeaderId()))
                return;

            TimeoutId timeout = UUID.nextUUID();
            pendingForCommit.put(request.getEntry(), timeout);

            trigger(new ReplicationPrepairCommitMessage.Response(self.getAddress(), request.getVodSource(), request.getTimeoutId(), request.getEntry().getId()), networkPort);

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
            if(pendingForCommit.containsKey(awaitingForCommitTimeout.getEntry()))
                pendingForCommit.remove(awaitingForCommitTimeout.getEntry());

        }
    };

    /**
     * Leader gains majority of responses and issues a request for a commit
     */
    final Handler<ReplicationPrepairCommitMessage.Response> handlePrepairCoomitResponse = new Handler<ReplicationPrepairCommitMessage.Response>() {
        @Override
        public void handle(ReplicationPrepairCommitMessage.Response response) {
            TimeoutId timeout = response.getTimeoutId();

            CancelTimeout ct = new CancelTimeout(timeout);
            trigger(ct, timerPort);

            ReplicationCount replicationCount = replicationRequests.get(timeout);
            if(replicationCount == null  || !replicationCount.incrementAndCheckReceived())
                return;

            IndexEntry entryToCommit = replicationCount.getEntry();
            TimeoutId commitTimeout = UUID.nextUUID();
            commitRequests.put(commitTimeout, replicationCount);
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

            if(leaderIds.isEmpty())
                return;

            ByteBuffer idBuffer = ByteBuffer.allocate(8);
            idBuffer.putLong(id);
            try {
                if(!verifyRSASignature(idBuffer.array(), leaderIds.get(leaderIds.size()-1), request.getSignature()))
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
                if(entry.getId() == id) {
                    toCommit = entry;
                    break;
                }
            }

            if(toCommit == null)
                return;

            CancelTimeout ct = new CancelTimeout(pendingForCommit.get(toCommit));
            trigger(ct, timerPort);

            try {
                addEntryLocal(toCommit);
                trigger(new ReplicationCommitMessage.Response(self.getAddress(), request.getVodSource(), request.getTimeoutId(), toCommit.getId()), networkPort);
                pendingForCommit.remove(toCommit);

                long maxStoredId = getMaxStoredId();

                if(toCommit.getId() - maxStoredId == config.getNumPartitions())
                    return;

                ArrayList<Long> missingIds = new ArrayList<Long>();
                long currentMissingValue = maxStoredId < 0 ? 0 : maxStoredId + 1;
                while(currentMissingValue < toCommit.getId()) {
                    missingIds.add(currentMissingValue);
                    currentMissingValue++;
                }

                if(missingIds.size() > 0) {
                    RepairMessage.Request repairMessage = new RepairMessage.Request(self.getAddress(), request.getVodSource(), request.getTimeoutId(), missingIds.toArray(new Long[missingIds.size()]));
                    trigger(repairMessage, networkPort);
                }
            } catch (IOException e) {
                logger.error(self.getId() + " " + e.getMessage());
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

            if(!commitRequests.containsKey(commitId))
                return;

            ReplicationCount replicationCount = commitRequests.get(commitId);
            try {
                addEntryLocal(replicationCount.getEntry());

                trigger(new AddIndexEntryMessage.Response(self.getAddress(), replicationCount.getSource(), response.getTimeoutId()), networkPort);

                commitRequests.remove(commitId);
                Snapshot.addIndexEntryId(self.getAddress().getPartitionId(), replicationCount.getEntry().getId());
            } catch (IOException e) {
                logger.error(self.getId() + " " + e.getMessage());
            }
        }
    };

    final Handler<CommitTimeout> handleCommitTimeout = new Handler<CommitTimeout>() {
        @Override
        public void handle(CommitTimeout commitTimeout) {
            if(commitRequests.containsKey(commitTimeout.getTimeoutId()))
                commitRequests.remove(commitTimeout.getTimeoutId());
        }
    };

    /**
     * Returns max stored id on a peer
     * @return max stored id on a peer
     */
    private long getMaxStoredId() {
        long currentIndexValue = lowestMissingIndexValue - 1;

        if(existingEntries.isEmpty() || currentIndexValue > Collections.max(existingEntries))
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
            IndexEntry[] missingEntries = new IndexEntry[request.getMissingIds().length];
            try {
                for(int i=0; i<request.getMissingIds().length; i++) {
                    //System.out.println(String.format("%s missing %s, but have %s", request.getVodSource().getId(), request.getMissingIds()[i], request.getFutureEntry().getId()));
                    IndexEntry entry = findById(request.getMissingIds()[i]);
                    if(entry != null) missingEntries[i] = entry;
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
                for(IndexEntry entry : response.getMissingEntries())
                    if(entry != null) addEntryLocal(entry);
            } catch (IOException e) {
                logger.error(self.getId() + " " + e.getMessage());
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

            if(!leader) return;

            trigger(new PublicKeyBroadcast(publicKey), publicKeyPort);
        }
    };

    /**
     * Stores leader public key if not repeated
     */
    final Handler<PublicKeyBroadcast> handlePublicKeyBroadcast = new Handler<PublicKeyBroadcast>() {
        @Override
        public void handle(PublicKeyBroadcast publicKeyBroadcast) {
            PublicKey key = publicKeyBroadcast.getPublicKey();

            if(!leaderIds.contains(key)) {
                if(leaderIds.size() == config.getMaxLeaderIdHistorySize())
                    leaderIds.remove(leaderIds.get(0));
                leaderIds.add(key);
            }
        }
    };

    /**
     * Send a search request for a given search pattern to one node in each
     * partition except the local partition.
     *
     * @param pattern the search pattern
     */
    private void startSearch(SearchPattern pattern) {
        searchRequest = new LocalSearchRequest(pattern);
        searchIndex = new RAMDirectory();

        // Can't open the index before committing a writer once
        IndexWriter writer;
        try {
            writer = new IndexWriter(searchIndex, indexWriterConfig);
            writer.commit();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        ScheduleTimeout rst = new ScheduleTimeout(config.getQueryTimeout());
        rst.setTimeoutEvent(new SearchMessage.RequestTimeout(rst, self.getId()));
        searchRequest.setTimeoutId((UUID) rst.getTimeoutEvent().getTimeoutId());
        trigger(rst, timerPort);

        trigger (new GradientRoutingPort.SearchRequest(pattern, searchRequest.getTimeoutId()), gradientRoutingPort);

        try {
            ArrayList<IndexEntry> result = searchLocal(index, pattern);
            addSearchResponse((IndexEntry[]) result.toArray(), self.getAddress().getPartitionId());
        } catch (IOException e) {
            java.util.logging.Logger.getLogger(Search.class.getName()).log(Level.SEVERE, null, e);
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
                ArrayList<IndexEntry> result = searchLocal(index, event.getPattern());

                trigger(new SearchMessage.Response(self.getAddress(), event.getVodSource(), event.getTimeoutId(), 0, 0, result.toArray(new IndexEntry[result.size()])), networkPort);
            } catch (IOException ex) {
                java.util.logging.Logger.getLogger(Search.class.getName()).log(Level.SEVERE, null, ex);
            } catch (IllegalSearchString illegalSearchString) {
                illegalSearchString.printStackTrace();
            }
        }
    };

    /**
     * Query the given index store with a given search pattern.
     *
     * @param index the {@link Directory} to search in
     * @param pattern the {@link SearchPattern} to use
     * @return a list of matching entries
     * @throws IOException if Lucene errors occur
     */
    private ArrayList<IndexEntry> searchLocal(Directory index, SearchPattern pattern) throws IOException {
        IndexReader reader = null;
        try {
            reader = DirectoryReader.open(index);
            IndexSearcher searcher = new IndexSearcher(reader);
            TopScoreDocCollector collector = TopScoreDocCollector.create(config.getHitsPerQuery(), true);
            searcher.search(pattern.getQuery(), collector);
            ScoreDoc[] hits = collector.topDocs().scoreDocs;

            ArrayList<IndexEntry> result = new ArrayList<IndexEntry>();
            for (int i = 0; i < hits.length; ++i) {
                int docId = hits[i].doc;
                Document d = searcher.doc(docId);
                result.add(createIndexEntry(d));
            }

            return result;
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                }
            }
        }
    }

    /**
     * Add the response to the search index store.
     */
    final Handler<SearchMessage.Response> handleSearchResponse = new Handler<SearchMessage.Response>() {
        @Override
        public void handle(SearchMessage.Response event) {
            if (searchRequest == null || event.getTimeoutId().equals(searchRequest.getTimeoutId()) == false) {
                return;
            }

            addSearchResponse(event.getResults(), event.getVodSource().getPartitionId());
        }
    };

    /**
     * Add all entries from a {@link SearchMessage.Response} to the search index.
     *
     * @param entries the entries to be added
     * @param partition the partition from which the entries originate from
     */
    private void addSearchResponse(IndexEntry[] entries, int partition) {
        if (searchRequest.hasResponded(partition)) {
            return;
        }

        try {
            addIndexEntries(searchIndex, entries);
        } catch (IOException e) {
            java.util.logging.Logger.getLogger(Search.class.getName()).log(Level.SEVERE, null, e);
        }

        searchRequest.addRespondedPartition(partition);
        if (searchRequest.getNumberOfRespondedPartitions() == config.getNumPartitions()) {
            CancelTimeout ct = new CancelTimeout(searchRequest.getTimeoutId());
            trigger(ct, timerPort);
            answerSearchRequest();
        }
    }

    /**
     * Answer a search request if the timeout occurred before all answers were
     * collected.
     */
    final Handler<SearchMessage.RequestTimeout> handleSearchTimeout = new Handler<SearchMessage.RequestTimeout>() {
        @Override
        public void handle(SearchMessage.RequestTimeout event) {
            answerSearchRequest();
        }
    };

    /**
     * Present the result to the user.
     */
    private void answerSearchRequest() {
        try {
            ArrayList<IndexEntry> result = searchLocal(searchIndex, searchRequest.getSearchPattern());
            // TODO present the result to the user
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Add the given {@link IndexEntry}s to the given Lucene directory
     *
     * @param index   the directory to which the given entries should be added
     * @param entries a collection of index entries to be added
     * @throws IOException in case the adding operation failed
     */
    private void addIndexEntries(Directory index, IndexEntry[] entries)
            throws IOException {
        IndexWriter writer = null;
        try {
            writer = new IndexWriter(index, indexWriterConfig);
            for (IndexEntry entry : entries) {
                addIndexEntry(writer, entry);
            }
            writer.commit();
        } finally {
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                }
            }
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
    private void addIndexEntry(Directory index, IndexEntry entry) throws IOException {
        IndexWriter writer = new IndexWriter(index, indexWriterConfig);
        addIndexEntry(writer, entry);
        writer.close();
    }

    /**
     * Add the given {@link IndexEntry} to the Lucene index using the given
     * writer.
     *
     * @param writer the writer used to add the {@link IndexEntry}
     * @param entry  the {@link IndexEntry} to be added
     * @throws IOException in case the adding operation failed
     */
    private void addIndexEntry(IndexWriter writer, IndexEntry entry) throws IOException {
        Document doc = new Document();
        doc.add(new LongField(IndexEntry.ID, entry.getId(), Field.Store.YES));
        doc.add(new StoredField(IndexEntry.URL, entry.getUrl()));
        doc.add(new TextField(IndexEntry.FILE_NAME, entry.getFileName(), Field.Store.YES));
        doc.add(new IntField(IndexEntry.CATEGORY, entry.getCategory().ordinal(), Field.Store.YES));
        doc.add(new TextField(IndexEntry.DESCRIPTION, entry.getDescription(), Field.Store.YES));
        doc.add(new StoredField(IndexEntry.HASH, entry.getHash()));
        if(entry.getLeaderId() == null)
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

        writer.addDocument(doc);
    };

    /**
     * Add a new {link {@link IndexEntry} to the local Lucene index.
     *
     * @param indexEntry the {@link IndexEntry} to be added
     * @throws IOException if the Lucene index fails to store the entry
     */
    private void addEntryLocal(IndexEntry indexEntry) throws IOException {
        if (indexEntry.getId() < lowestMissingIndexValue
                || existingEntries.contains(indexEntry.getId())) {
            return;
        }

        addIndexEntry(index, indexEntry);
        ((MsSelfImpl)self).incrementNumberOfIndexEntries();
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
            return new IndexEntry(Long.valueOf(d.get(IndexEntry.ID)),
                    d.get(IndexEntry.URL), d.get(IndexEntry.FILE_NAME),
                    IndexEntry.Category.values()[Integer.valueOf(d.get(IndexEntry.CATEGORY))],
                    d.get(IndexEntry.DESCRIPTION), d.get(IndexEntry.HASH), null);

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
        IndexEntry entry = new IndexEntry(Long.valueOf(d.get(IndexEntry.ID)),
                d.get(IndexEntry.URL), d.get(IndexEntry.FILE_NAME),
                IndexEntry.Category.values()[Integer.valueOf(d.get(IndexEntry.CATEGORY))],
                d.get(IndexEntry.DESCRIPTION), d.get(IndexEntry.HASH), pub);

        return entry;
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
        List<IndexEntry> indexEntries = findIdRange(id, id, 1);
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
    private List<IndexEntry> findIdRange(long min, long max, int limit) throws IOException {
        IndexReader reader = null;
        try {
            reader = DirectoryReader.open(index);
            IndexSearcher searcher = new IndexSearcher(reader);

            Query query = NumericRangeQuery.newLongRange(IndexEntry.ID, min, max, true, true);
            TopDocs topDocs = searcher.search(query, limit, new Sort(new SortField(IndexEntry.ID,
                    Type.LONG)));
            ArrayList<IndexEntry> indexEntries = new ArrayList<IndexEntry>();
            for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                Document d = searcher.doc(scoreDoc.doc);
                indexEntries.add(createIndexEntry(d));
            }

            return indexEntries;
        } finally {
            if (reader != null) {
                reader.close();
            }
        }
    }

    /**
     * Generates a SHA-1 hash on IndexEntry and signs it with a private key
     * @param newEntry
     * @return signed SHA-1 key
     */
    private static String generateSignedHash(IndexEntry newEntry, PrivateKey privateKey) {
        if(newEntry.getLeaderId() == null)
            return null;

        byte[] urlBytes = newEntry.getUrl().getBytes(Charset.forName("UTF-8"));
        byte[] fileNameBytes = newEntry.getFileName().getBytes(Charset.forName("UTF-8"));
        byte[] languageBytes = newEntry.getLanguage().getBytes(Charset.forName("UTF-8"));
        byte[] descriptionBytes = newEntry.getDescription().getBytes(Charset.forName("UTF-8"));

        ByteBuffer dataBuffer = ByteBuffer.allocate(8 * 3 + 4 + urlBytes.length + fileNameBytes.length +
                languageBytes.length + descriptionBytes.length);
        dataBuffer.putLong(newEntry.getId());
        dataBuffer.putLong(newEntry.getFileSize());
        dataBuffer.putLong(newEntry.getUploaded().getTime());
        dataBuffer.putInt(newEntry.getCategory().ordinal());
        dataBuffer.put(urlBytes);
        dataBuffer.put(fileNameBytes);
        dataBuffer.put(languageBytes);
        dataBuffer.put(descriptionBytes);

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
        if(newEntry.getLeaderId() == null)
            return false;

        byte[] urlBytes = newEntry.getUrl().getBytes(Charset.forName("UTF-8"));
        byte[] fileNameBytes = newEntry.getFileName().getBytes(Charset.forName("UTF-8"));
        byte[] languageBytes = newEntry.getLanguage().getBytes(Charset.forName("UTF-8"));
        byte[] descriptionBytes = newEntry.getDescription().getBytes(Charset.forName("UTF-8"));

        ByteBuffer dataBuffer = ByteBuffer.allocate(8 * 3 + 4 + urlBytes.length + fileNameBytes.length +
                languageBytes.length + descriptionBytes.length);
        dataBuffer.putLong(newEntry.getId());
        dataBuffer.putLong(newEntry.getFileSize());
        dataBuffer.putLong(newEntry.getUploaded().getTime());
        dataBuffer.putInt(newEntry.getCategory().ordinal());
        dataBuffer.put(urlBytes);
        dataBuffer.put(fileNameBytes);
        dataBuffer.put(languageBytes);
        dataBuffer.put(descriptionBytes);

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
                    + Character.digit(s.charAt(i+1), 16));
        }
        return data;
    }
}
