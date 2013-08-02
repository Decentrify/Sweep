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
import se.sics.gvod.common.VodDescriptor;
import se.sics.gvod.config.SearchConfiguration;
import se.sics.gvod.croupier.PeerSamplePort;
import se.sics.gvod.croupier.events.CroupierSample;
import se.sics.gvod.net.VodNetwork;
import se.sics.gvod.timer.*;
import se.sics.gvod.timer.Timer;
import se.sics.gvod.timer.UUID;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.ms.gradient.LeaderRequestPort;
import se.sics.ms.gradient.LeaderStatusPort;
import se.sics.ms.gradient.PublicKeyBroadcast;
import se.sics.ms.peer.IndexPort;
import se.sics.ms.peer.IndexPort.AddIndexSimulated;
import se.sics.ms.gradient.PublicKeyPort;
import se.sics.ms.search.Timeouts.*;
import se.sics.ms.snapshot.Snapshot;
import se.sics.peersearch.exceptions.IllegalSearchString;
import se.sics.peersearch.messages.*;
import se.sics.peersearch.types.IndexEntry;
import se.sics.peersearch.types.SearchPattern;
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

    Positive<IndexPort> indexPort = positive(IndexPort.class);
    Positive<VodNetwork> networkPort = positive(VodNetwork.class);
    Positive<Timer> timerPort = positive(Timer.class);
    Positive<PeerSamplePort> croupierSamplePort = positive(PeerSamplePort.class);
    Positive<LeaderRequestPort> leaderRequestPort = positive(LeaderRequestPort.class);
    Negative<LeaderStatusPort> leaderStatusPort = negative(LeaderStatusPort.class);
    Negative<PublicKeyPort> publicKeyPort = negative(PublicKeyPort.class);

    private static final Logger logger = LoggerFactory.getLogger(Search.class);
    private Self self;
    private SearchConfiguration config;
    private boolean leader;
    // The last smallest missing index number.
    private long oldestMissingIndexValue;
    // Set of existing entries higher than the oldestMissingIndexValue
    private SortedSet<Long> existingEntries;
    // The last id used for adding new entries in case this node is the leader
    private long nextInsertionId;
    // Data structure to keep track of acknowledgments for newly added indexes
    private Map<TimeoutId, ReplicationCount> replicationRequests;
    private Random random;
    // The number of the local partition
    private int partition;
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
    private HashMap<TimeoutId, IndexEntry> pendingForCommit = new HashMap<TimeoutId, IndexEntry>();

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

    public Search() {
        subscribe(handleInit, control);
        subscribe(handleCroupierSample, croupierSamplePort);
        subscribe(handleAddIndexSimulated, indexPort);
        subscribe(handleIndexExchangeRequest, networkPort);
        subscribe(handleIndexExchangeResponse, networkPort);
        subscribe(handleAddIndexEntryRequest, networkPort);
        subscribe(handleAddIndexEntryResponse, networkPort);
        subscribe(handleReplicationRequest, networkPort);
        subscribe(handleReplicationResponse, networkPort);
        subscribe(handleSearchRequest, networkPort);
        subscribe(handleSearchResponse, networkPort);
        subscribe(handleSearchTimeout, timerPort);
        subscribe(handleReplicationTimeout, timerPort);
        subscribe(handleAddRequestTimeout, timerPort);
        subscribe(handleGapTimeout, timerPort);
        subscribe(handleRecentRequestsGcTimeout, timerPort);
        subscribe(handleLeaderStatus, leaderStatusPort);
        subscribe(repairRequestHandler, networkPort);
        subscribe(repairResponseHandler, networkPort);
        subscribe(publicKeyBroadcastHandler, publicKeyPort);
        subscribe(prepairCommitHandler, networkPort);
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
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }

            routingTable = new HashMap<Integer, TreeSet<VodDescriptor>>(config.getNumPartitions());
            partition = self.getId() % config.getNumPartitions();
            nextInsertionId = partition;
            replicationRequests = new HashMap<TimeoutId, ReplicationCount>();
            random = new Random(init.getConfiguration().getSeed());
            oldestMissingIndexValue = partition;
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

            // TODO check if still needed
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
        IndexSearcher searcher = null;
        try {
            reader = DirectoryReader.open(index);
            searcher = new IndexSearcher(reader);

            boolean continuous = true;
            // TODO check which limit performs well
            int readLimit = 20000;
            // Would not terminate in case it reaches the limit of long ;)
            for (long i = 0; ; i += readLimit) {
                Query query = NumericRangeQuery.newLongRange(IndexEntry.ID, i, i + readLimit, true,
                        false);
                TopDocs topDocs = searcher.search(query, readLimit, new Sort(new SortField(
                        IndexEntry.ID, Type.LONG)));

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
                    if (ids[0] != partition && oldestMissingIndexValue != ids[0]) {
                        continuous = false;
                        for (Long id : ids) {
                            existingEntries.add(id);
                        }
                    } else {
                        // Search for gaps between the given ids
                        for (int j = 0; j < ids.length; j++) {
                            oldestMissingIndexValue = ids[j]
                                    + config.getNumPartitions();
                            // If a gap was found add higher ids to the existing
                            // entries
                            if (j + 1 < ids.length && ids[j] + 1 != ids[j + 1]) {
                                continuous = false;
                                for (int k = j + 1; k < ids.length; k++) {
                                    existingEntries.add(ids[k]);
                                }
                                break;
                            }
                        }
                    }
                } else {
                    for (int j = 0; j < hits.length; j++) {
                        existingEntries.add(Long.valueOf(searcher.doc(hits[j].doc).get(
                                IndexEntry.ID)));
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
     * Handle samples from Croupier. Use them to update the routing tables and
     * issue an index exchange with another node.
     */
    final Handler<CroupierSample> handleCroupierSample = new Handler<CroupierSample>() {
        @Override
        public void handle(CroupierSample event) {
            // receive a new list of neighbors
            List<VodDescriptor> peers = event.getNodes();
            if (peers.isEmpty()) {
                return;
            }

            // update routing tables
            for (VodDescriptor p : event.getNodes()) {
                int samplePartition = p.getVodAddress().getId() % config.getNumPartitions();
                TreeSet<VodDescriptor> nodes = routingTable.get(samplePartition);
                if (nodes == null) {
                    nodes = new TreeSet<VodDescriptor>(peerAgeComparator);
                    routingTable.put(samplePartition, nodes);
                }

                // Increment age
                for (VodDescriptor peer : nodes) {
                    peer.incrementAndGetAge();
                }

                // Note - this might replace an existing entry
                nodes.add(p);
                // keep the freshest descriptors in this partition
                while (nodes.size() > config.getMaxNumRoutingEntries()) {
                    nodes.pollLast();
                }
            }

            // Exchange index with one sample from our partition
            TreeSet<VodDescriptor> bucket = routingTable.get(partition);
            if (bucket != null) {
                int n = random.nextInt(bucket.size());

                trigger(new IndexExchangeMessage.Request(self.getAddress(), ((VodDescriptor) bucket.toArray()[n]).getVodAddress(),
                        UUID.nextUUID(), oldestMissingIndexValue, existingEntries.toArray(new Long[existingEntries
                        .size()]), 0, 0), networkPort);
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
                    indexEntries.addAll(findIdRange(lastId,
                            i - config.getNumPartitions(),
                            config.getMaxExchangeCount() - indexEntries.size()));
                    lastId = i + config.getNumPartitions();

                    if (indexEntries.size() >= config.getMaxExchangeCount()) {
                        break;
                    }
                }

                // In case there is some space left search for more
                if (indexEntries.size() < config.getMaxExchangeCount()) {
                    indexEntries.addAll(findIdRange(lastId, Long.MAX_VALUE,
                            config.getMaxExchangeCount() - indexEntries.size()));
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
     * Handler executed in the role of the leader. Create a new id and search
     * for a the according bucket in the routing table. If it does not include
     * enough nodes to satisfy the replication requirements then create a new id
     * and try again. Send a {@link ReplicationMessage} request to a number of nodes as
     * specified in the config file and schedule a timeout to wait for
     * responses. The adding operation will be acknowledged if either all nodes
     * responded to the {@link ReplicationMessage} request or the timeout occurred and
     * enough nodes, as specified in the config, responded.
     */
    final Handler<AddIndexEntryMessage.Request> handleAddIndexEntryRequest = new Handler<AddIndexEntryMessage.Request>() {
        @Override
        public void handle(AddIndexEntryMessage.Request event) {
            if (!leader) {
                return;
            }

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

            // TODO PREPAIR PHAISE

            avaitingForPrepairResponse.put(event.getTimeoutId(), newEntry);
            replicationRequests.put(event.getTimeoutId(), new ReplicationCount(event.getVodSource(), config.getReplicationMinimum()));


            //addEntryLocal(newEntry);

            TreeSet<VodDescriptor> bucket = routingTable.get(partition);
            int i = bucket.size() > config.getReplicationMaximum() ? config.getReplicationMaximum() : bucket.size();
            for (VodDescriptor peer : bucket) {
                if (i == 0) {
                    break;
                }
                trigger(new PrepairCommitMessage.Request(self.getAddress(), peer.getVodAddress(), event.getTimeoutId(), newEntry), networkPort);
                //trigger(new ReplicationMessage.Request(self.getAddress(), peer.getVodAddress(), event.getTimeoutId(), newEntry, 0, 0), networkPort);
                i--;
            }

            ScheduleTimeout rst = new ScheduleTimeout(config.getReplicationTimeout());
            rst.setTimeoutEvent(new ReplicationTimeout(rst, self.getId()));
            rst.getTimeoutEvent().setTimeoutId(event.getTimeoutId());
            trigger(rst, timerPort);

            //Snapshot.setLastId(id);
        }
    };

    final Handler<PrepairCommitMessage.Request> prepairCommitHandler = new Handler<PrepairCommitMessage.Request>() {
        @Override
        public void handle(PrepairCommitMessage.Request request) {
            IndexEntry entry = request.getEntry();
            if(!isSignatureValid(entry) || !leaderIds.contains(entry.getLeaderId()))
                return;

            pendingForCommit.put(request.getTimeoutId(), request.getEntry());

            trigger(new PrepairCommitMessage.Response(self.getAddress(), request.getVodSource(), request.getTimeoutId(), request.getEntry().getId()), networkPort);
        }
    };

    private static boolean isSignatureValid(IndexEntry newEntry) {
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
            MessageDigest digest = MessageDigest.getInstance("SHA-1");
            String sha1 = byteArray2Hex(digest.digest(dataBuffer.array()));
            System.out.println(sha1);

            Signature instance = Signature.getInstance("SHA1withRSA");
            instance.initVerify(newEntry.getLeaderId());
            instance.update(sha1.getBytes());
            return instance.verify(hexStringToByteArray(newEntry.getHash()));
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (SignatureException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (InvalidKeyException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

        return false;
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
     * When receiving a replicate message from the leader, add the entry to the
     * local store and send an acknowledgment if all previous entries are stored. If no - ask
     * for them.
    */
    final Handler<ReplicationMessage.Request> handleReplicationRequest = new Handler<ReplicationMessage.Request>() {
        @Override
        public void handle(ReplicationMessage.Request event) {
            try {


                addEntryLocal(event.getIndexEntry());
                ReplicationMessage.Response msg = new ReplicationMessage.Response(self.getAddress(), event.getVodSource(), event.getTimeoutId());
                trigger(msg, networkPort);

                int numOfPartitions = config.getNumPartitions();
                long maxStoredId = getMaxStoredId(numOfPartitions);

                if(event.getIndexEntry().getId() - maxStoredId == numOfPartitions)
                    return;

                ArrayList<Long> missingIds = new ArrayList<Long>();
                long currentMissingValue = maxStoredId < 0 ? partition : maxStoredId + numOfPartitions;
                while(currentMissingValue < event.getIndexEntry().getId()) {
                    missingIds.add(currentMissingValue);
                    currentMissingValue += numOfPartitions;
                }

                if(missingIds.size() > 0) {
                    RepairMessage.Request repairMessage = new RepairMessage.Request(self.getAddress(), event.getVodSource(), event.getTimeoutId(), event.getIndexEntry(), missingIds.toArray(new Long[missingIds.size()]));
                    trigger(repairMessage, networkPort);
                }
            } catch (IOException e) {
                logger.error(self.getId() + " " + e.getMessage());
            }
        }
    };

    /**
     * Returns max stored id on a peer
     * @param numOfPartitions
     * @return max stored id on a peer
     */
    private long getMaxStoredId(int numOfPartitions) {
        long normalizedMissingIndexValue = oldestMissingIndexValue - partition;
        if(normalizedMissingIndexValue == 0) {
            //it the first value that is missing, return -1
            return -1;
        }

        long currentIndexValue = oldestMissingIndexValue - numOfPartitions;

        if(existingEntries.isEmpty() || currentIndexValue > Collections.max(existingEntries))
            return currentIndexValue;

        return Collections.max(existingEntries);
    }

    /**
     * Handles situations then a peer in the leader group is behind in the updates during add operation
     * and asks for missing data
     */
    Handler<RepairMessage.Request> repairRequestHandler = new Handler<RepairMessage.Request>() {
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

            RepairMessage.Response msg = new RepairMessage.Response(self.getAddress(), request.getVodSource(), request.getTimeoutId(), request.getFutureEntry(), missingEntries);
            trigger(msg, networkPort);
        }
    };

    /**
     * Handles missing data on the peer from the leader group when adding a new entry, but the peer is behind
     * with the updates
     */
    Handler<RepairMessage.Response> repairResponseHandler = new Handler<RepairMessage.Response>() {
        @Override
        public void handle(RepairMessage.Response response) {
            try {
                for(IndexEntry entry : response.getMissingEntries())
                    addEntryLocal(entry);

                addEntryLocal(response.getFutureEntry());
            } catch (IOException e) {
                logger.error(self.getId() + " " + e.getMessage());
            }
        }
    };

    /**
     * As the leader, add an {@link ReplicationMessage.Request} to the according
     * request and issue the response if the replication constraints were
     * satisfied.
     */
    final Handler<ReplicationMessage.Response> handleReplicationResponse = new Handler<ReplicationMessage.Response>() {
        @Override
        public void handle(ReplicationMessage.Response event) {
            if (!leader) {
                return;
            }

            ReplicationCount replicationCount = replicationRequests.get(event.getTimeoutId());
            if (replicationCount != null && replicationCount.incrementAndCheckReceived()) {

                trigger(new AddIndexEntryMessage.Response(self.getAddress(), replicationCount.getSource(), event.getTimeoutId()), networkPort);
                replicationRequests.remove(event.getTimeoutId());
            }
        }
    };

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
     * Add the response to the search index store.
     */
    final Handler<SearchMessage.Response> handleSearchResponse = new Handler<SearchMessage.Response>() {
        @Override
        public void handle(SearchMessage.Response event) {
            if (searchRequest == null || event.getTimeoutId().equals(searchRequest.getTimeoutId()) == false) {
                return;
            }

            addSearchResponse(event.getResults());
        }
    };

    /**
     * Answer a search request if the timeout occurred before all answers were
     * collected.
     */
    final Handler<SearchTimeout> handleSearchTimeout = new Handler<SearchTimeout>() {
        @Override
        public void handle(SearchTimeout event) {
            answerSearchRequest();
        }
    };

    /**
     * Only execute in the role of the leader. Garbage collect replication
     * requests if the constraints could not be satisfied in time. In this case,
     * no acknowledgment is sent to the client.
     */
    final Handler<ReplicationTimeout> handleReplicationTimeout = new Handler<ReplicationTimeout>() {
        @Override
        public void handle(ReplicationTimeout event) {
            // TODO We could send a message to the client here that we are
            // unsure if it worked. The client can then search the entry later
            // to check this and insert it again if necessary.

            // Garbage collect entry
            replicationRequests.remove(event.getTimeoutId());
        }
    };

    /**
     * No acknowledgment for an issued {@link AddIndexEntryMessage.Request} was received
     * in time. Try to add the entry again or respons with failure to the web client.
     */
    final Handler<AddRequestTimeout> handleAddRequestTimeout = new Handler<AddRequestTimeout>() {
        @Override
        public void handle(AddRequestTimeout event) {
            if (event.reachedRetryLimit()) {
                // TODO inform the user
            } else {
                event.incrementTries();
                ScheduleTimeout rst = new ScheduleTimeout(config.getAddTimeout());
                rst.setTimeoutEvent(event);
                addEntryGlobal(event.getEntry(), rst);
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
     * The entry for a detected gap was not added in time.
     */
    final Handler<GapTimeout> handleGapTimeout = new Handler<GapTimeout>() {
        @Override
        public void handle(GapTimeout event) {
            try {
                if (entryExists(event.getId()) == false) {
                    // TODO implement new gap check
                }
            } catch (IOException e) {
                java.util.logging.Logger.getLogger(Search.class.getName()).log(Level.SEVERE, null,
                        e);
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
    final Handler<PublicKeyBroadcast> publicKeyBroadcastHandler = new Handler<PublicKeyBroadcast>() {
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

        int i = 0;
        for (SortedSet<VodDescriptor> bucket : routingTable.values()) {
            // Skip local partition
            if (i == partition) {
                i++;
                continue;
            }

            int n = random.nextInt(bucket.size());

            trigger(new SearchMessage.Request(self.getAddress(), ((VodDescriptor) bucket.toArray()[n]).getVodAddress(), searchRequest.getTimeoutId(), pattern), networkPort);
            searchRequest.incrementNodesQueried();
            i++;
        }

        ScheduleTimeout rst = new ScheduleTimeout(config.getQueryTimeout());
        rst.setTimeoutEvent(new SearchTimeout(rst, self.getId()));
        searchRequest.setTimeoutId((UUID) rst.getTimeoutEvent().getTimeoutId());
        trigger(rst, timerPort);

        // Add result form local partition
        try {
            ArrayList<IndexEntry> result = searchLocal(index, pattern);
            searchRequest.incrementNodesQueried();
            addSearchResponse((IndexEntry[]) result.toArray());
        } catch (IOException e) {
            java.util.logging.Logger.getLogger(Search.class.getName()).log(Level.SEVERE, null, e);
        }
    }

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
     * Add a new {link {@link IndexEntry} to the system and schedule a timeout
     * to wait for the acknowledgment.
     *
     * @param entry the {@link IndexEntry} to be added
     */
    private void addEntryGlobal(IndexEntry entry) {
        // Limit the time to wait for responses and answer the web request
        ScheduleTimeout rst = new ScheduleTimeout(config.getAddTimeout());
        rst.setTimeoutEvent(new AddRequestTimeout(rst, self.getId(), config.getRetryCount(), entry));
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
        trigger(new LeaderRequestPort.AddIndexEntryRequest(entry, timeout.getTimeoutEvent().getTimeoutId()), leaderRequestPort);
    }

    /**
     * Add a new {link {@link IndexEntry} to the local Lucene index.
     *
     * @param indexEntry the {@link IndexEntry} to be added
     * @throws IOException if the Lucene index fails to store the entry
     */
    private void addEntryLocal(IndexEntry indexEntry) throws IOException {
        if (indexEntry.getId() < oldestMissingIndexValue
                || existingEntries.contains(indexEntry.getId())) {
            return;
        }

        addIndexEntry(index, indexEntry);
        Snapshot.incNumIndexEntries(self.getAddress());

        // Cancel gap detection timeouts for the given index
        TimeoutId timeoutId = gapTimeouts.get(indexEntry.getId());
        if (timeoutId != null) {
            CancelTimeout ct = new CancelTimeout(timeoutId);
            trigger(ct, timerPort);
        }

        if (indexEntry.getId() == oldestMissingIndexValue) {
            // Search for the next missing index id
            do {
                existingEntries.remove(oldestMissingIndexValue);
                oldestMissingIndexValue += config.getNumPartitions();
            } while (existingEntries.contains(oldestMissingIndexValue));
        } else if (indexEntry.getId() > oldestMissingIndexValue) {
            existingEntries.add(indexEntry.getId());

            // Suspect all missing entries less than the new as gaps
            for (long i = oldestMissingIndexValue; i < indexEntry.getId(); i = i
                    + config.getNumPartitions()) {
                if (gapTimeouts.containsKey(i)) {
                    continue;
                }

                // This might be a gap so start a timeouts
                ScheduleTimeout rst = new ScheduleTimeout(config.getGapTimeout());
                rst.setTimeoutEvent(new GapTimeout(rst, self.getId(), i));
                gapTimeouts.put(indexEntry.getId(), (UUID) rst.getTimeoutEvent().getTimeoutId());
                trigger(rst, timerPort);
            }
        }
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
     * @return a new id for a new {@link IndexEntry}
     */
    private long getNextInsertionId() {
        long id = nextInsertionId;
        nextInsertionId += config.getNumPartitions();
        return id;
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
     * Add all entries from a {@link SearchMessage.Response} to the search index.
     *
     * @param entries the entries to be added
     */
    private void addSearchResponse(IndexEntry[] entries) {
        try {
            addIndexEntries(searchIndex, entries);
        } catch (IOException e) {
            java.util.logging.Logger.getLogger(Search.class.getName()).log(Level.SEVERE, null, e);
        }

        searchRequest.incrementReceived();
        if (searchRequest.receivedAll()) {
            CancelTimeout ct = new CancelTimeout(searchRequest.getTimeoutId());
            trigger(ct, timerPort);
            answerSearchRequest();
        }
    }

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
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (InvalidKeySpecException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        IndexEntry entry = new IndexEntry(Long.valueOf(d.get(IndexEntry.ID)),
                d.get(IndexEntry.URL), d.get(IndexEntry.FILE_NAME),
                IndexEntry.Category.values()[Integer.valueOf(d.get(IndexEntry.CATEGORY))],
                d.get(IndexEntry.DESCRIPTION), d.get(IndexEntry.HASH), pub);

        return entry;
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
            MessageDigest digest = MessageDigest.getInstance("SHA-1");
            String sha1 = byteArray2Hex(digest.digest(dataBuffer.array()));
            System.out.println(sha1);

            Signature instance = Signature.getInstance("SHA1withRSA");
            instance.initSign(privateKey);
            instance.update((sha1).getBytes());
            byte[] signature = instance.sign();
            return byteArray2Hex(signature);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (SignatureException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (InvalidKeyException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

        return null;
    }

    private static String byteArray2Hex(final byte[] hash) {
        Formatter formatter = new Formatter();
        for (byte b : hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}
