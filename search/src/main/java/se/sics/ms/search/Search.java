package se.sics.ms.search;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.logging.Level;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopScoreDocCollector;
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
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.web.Web;
import se.sics.kompics.web.WebRequest;
import se.sics.kompics.web.WebResponse;
import se.sics.peersearch.exceptions.IllegalSearchString;
import se.sics.peersearch.messages.*;
import se.sics.peersearch.types.IndexEntry;
import se.sics.peersearch.types.SearchPattern;
import se.sics.ms.peer.IndexPort;
import se.sics.ms.peer.IndexPort.AddIndexSimulated;
import se.sics.ms.search.Timeouts.AddRequestTimeout;
import se.sics.ms.search.Timeouts.GapDetectionTimeout;
import se.sics.ms.search.Timeouts.GapTimeout;
import se.sics.ms.search.Timeouts.RecentRequestsGcTimeout;
import se.sics.ms.search.Timeouts.ReplicationTimeout;
import se.sics.ms.search.Timeouts.SearchTimeout;
import se.sics.ms.gradient.LeaderRequest.AddIndexEntry;
import se.sics.ms.gradient.LeaderRequest.GapCheck;
import se.sics.ms.gradient.RoutedEventsPort;
import se.sics.ms.peer.PeerDescriptor;
import se.sics.ms.snapshot.Snapshot;


/**
 * This class handles the storing, adding and searching for indexes. It acts in
 * two different modes depending on if it the executing node was elected leader
 * or not, although it doesn't know about the leader status. Gradient knows
 * about the leader status and only forwards according messages to this
 * component in case the local node is elected leader.
 * 
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
	Positive<RoutedEventsPort> routedEventsPort = positive(RoutedEventsPort.class);

	private static final Logger logger = LoggerFactory.getLogger(Search.class);
	private Self self;
	private SearchConfiguration config;
	// The last smallest missing index number.
	private long oldestMissingIndexValue;
	// Set of existing entries higher than the oldestMissingIndexValue
	private SortedSet<Long> existingEntries;
	// The last id used for adding new entries in case this node is the leader
	private long lastInsertionId;
	// Data structure to keep track of acknowledgments for newly added indexes
	private Map<UUID, ReplicationCount> replicationRequests;
	private Random random;
	// The number of the local partition
	private int partition;
	// Structure that maps index ids to UUIDs of open gap timeouts
	private Map<Long, UUID> gapTimeouts;

	/**
	 * Enum describing the status of the gap detection process.
	 */
	private enum GapStatus {
		UNDECIDED, TRUE, FALSE
	}

	// Maps index ids for currently issued gap detections to their status
	private Map<Long, GapStatus> gapDetections;

	// Set of recent add requests to avoid duplication
	private Map<UUID, Long> recentRequests;

	// Apache Lucene used for searching
	private StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_42);
	private Directory index;
	private IndexWriterConfig indexWriterConfig = new IndexWriterConfig(Version.LUCENE_42, analyzer);

	// Lucene variables used to store and search in collected answers
	private LocalSearchRequest searchRequest;
	private Directory searchIndex;

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
		subscribe(handleIndexUpdateRequest, networkPort);
		subscribe(handleIndexUpdateResponse, networkPort);
		subscribe(handleAddIndexEntry, routedEventsPort);
		subscribe(handleIndexEntryAdded, networkPort);
		subscribe(handleReplicate, networkPort);
		subscribe(handleReplicationConfirmation, networkPort);
		subscribe(handleSearchRequest, networkPort);
		subscribe(handleSearchResponse, networkPort);
		subscribe(handleSearchTimeout, timerPort);
		subscribe(handleReplicationTimeout, timerPort);
		subscribe(handleAddRequestTimeout, timerPort);
		subscribe(handleGapTimeout, timerPort);
		subscribe(handleGapCheck, routedEventsPort);
		subscribe(handleRecentRequestsGcTimeout, timerPort);
	}

	/**
	 * Initialize the component.
	 */
	Handler<SearchInit> handleInit = new Handler<SearchInit>() {
		public void handle(SearchInit init) {
			self = init.getSelf();
			config = init.getConfiguration();
			routingTable = new HashMap<Integer, TreeSet<VodDescriptor>>(
					config.getNumPartitions());
			lastInsertionId = -1;
			replicationRequests = new HashMap<UUID, ReplicationCount>();
			random = new Random(init.getConfiguration().getSeed());
			partition = self.getId() % config.getNumPartitions();
			oldestMissingIndexValue = partition;
			existingEntries = new TreeSet<Long>();
			gapTimeouts = new HashMap<Long, UUID>();
			gapDetections = new HashMap<Long, Search.GapStatus>();

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

			recentRequests = new HashMap<UUID, Long>();
			// Garbage collect the data structure
			SchedulePeriodicTimeout rst = new SchedulePeriodicTimeout(
					config.getRecentRequestsGcInterval(),
					config.getRecentRequestsGcInterval());
			rst.setTimeoutEvent(new RecentRequestsGcTimeout(rst));
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
	 * @throws IOException
	 *             in case errors occur while reading the index
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
			for (long i = 0;; i += readLimit) {
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
	 * Handle samples from Cyclon. Use them to update the routing tables and
	 * issue an index exchange with another node.
	 */
	Handler<CroupierSample> handleCroupierSample = new Handler<CroupierSample>() {
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

                trigger(new IndexExchangeMessage.Request(self.getAddress(), ((VodDescriptor)bucket.toArray()[n]).getVodAddress(),
                        UUID.nextUUID(), oldestMissingIndexValue, existingEntries.toArray(new Long[existingEntries
                        .size()]), 0,0), networkPort);
			}
		}
	};

	/**
	 * Add index entries for the simulator.
	 */
	Handler<AddIndexSimulated> handleAddIndexSimulated = new Handler<AddIndexSimulated>() {
		@Override
		public void handle(AddIndexSimulated event) {
            addEntryGlobal(event.getEntry(), (UUID)UUID.nextUUID());
		}
	};

	/**
	 * Add all entries received from another node to the local index store.
	 */
	Handler<IndexExchangeMessage.Response> handleIndexUpdateResponse = new Handler<IndexExchangeMessage.Response>() {
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
	Handler<IndexExchangeMessage.Request> handleIndexUpdateRequest = new Handler<IndexExchangeMessage.Request>() {
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

                trigger(new IndexExchangeMessage.Response(self.getAddress(), event.getVodSource(), event.getTimeoutId(), (IndexEntry[])indexEntries.toArray(), 0, 0), networkPort);
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
	Handler<AddIndexEntryMessage.Request> handleAddIndexEntry = new Handler<AddIndexEntryMessage.Request>() {
		@Override
		public void handle(AddIndexEntryMessage.Request event) {
			if (recentRequests.containsKey(event.getId())) {
				return;
			}
			recentRequests.put(event.getId(), System.currentTimeMillis());

			try {
				if (routingTable.isEmpty()) {
					// There's nothing we can do here
					return;
				}

				// Search the next id and a non-empty bucket an place the entry
				// there
				IndexEntry newEntry = event.getEntry();
				long id;
				int entryPartition;
				TreeSet<VodDescriptor> bucket;
				int i = routingTable.size();
				do {
					id = getCurrentInsertionId();
					entryPartition = (int) (id % config.getNumPartitions());
					bucket = routingTable.get(entryPartition);
					i--;
				} while ((bucket == null || config.getReplicationMinimum() > bucket
						.size()) && i > 0);

				// There is nothing we can do
				if (bucket == null || config.getReplicationMinimum() > bucket.size()) {
					return;
				}

				newEntry.setId(id);
				if (entryPartition == partition) {
					addEntryLocal(newEntry);
				}

				replicationRequests.put(event.getId(), new ReplicationCount(event.getVodSource(),
						config.getReplicationMinimum()));

				i = bucket.size() > config.getReplicationMaximum() ? config
						.getReplicationMaximum() : bucket.size();
				for (VodDescriptor peer : bucket) {
					if (i == 0) {
						break;
					}
                    trigger(new ReplicationMessage.Request(self.getAddress(), peer.getVodAddress(), event.getTimeoutId(), event.getId(), newEntry, 0,0), networkPort);
					i--;
				}

				ScheduleTimeout rst = new ScheduleTimeout(
						config.getReplicationTimeout());
				rst.setTimeoutEvent(new ReplicationTimeout(rst, event.getId()));
				trigger(rst, timerPort);

				Snapshot.setLastId(id);
			} catch (IOException e) {
				logger.error(self.getId() + " " + e.getMessage());
			}
		}
	};

	/**
	 * An index entry has been successfully added.
	 */
	Handler<AddIndexEntryMessage.Response> handleIndexEntryAdded = new Handler<AddIndexEntryMessage.Response>() {
		@Override
		public void handle(AddIndexEntryMessage.Response event) {
            // TODO inform user
			CancelTimeout ct = new CancelTimeout(event.getTimeoutId());
			trigger(ct, timerPort);
		}
	};

	/**
	 * When receiving a replicate messsage from the leader, add the entry to the
	 * local store and send an acknowledgment.
	 */
	Handler<ReplicationMessage.Request> handleReplicate = new Handler<ReplicationMessage.Request>() {
		@Override
		public void handle(ReplicationMessage.Request event) {
			try {
				addEntryLocal(event.getIndexEntry());

                trigger(new ReplicationMessage.Response(self.getAddress(), event.getVodDestination(), event.getTimeoutId(), event.getId()), networkPort);
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
	Handler<ReplicationMessage.Request> handleReplicationConfirmation = new Handler<ReplicationMessage.Request>() {
		@Override
		public void handle(ReplicationMessage.Request event) {
			ReplicationCount replicationCount = replicationRequests.get(event.getId());
			if (replicationCount != null && replicationCount.incrementAndCheckReceived()) {

                trigger(new AddIndexEntryMessage.Response(self.getAddress(), replicationCount.getSource(), event.getTimeoutId(), event.getIndexEntry(), event.getId(), 0, 0), networkPort);
				replicationRequests.remove(event.getId());
			}
		}
	};

	/**
	 * Query the local store with the given query string and send the response
	 * back to the inquirer.
	 */
	Handler<SearchMessage.Request> handleSearchRequest = new Handler<SearchMessage.Request>() {
		@Override
		public void handle(SearchMessage.Request event) {
			try {
				ArrayList<IndexEntry> result = searchLocal(event.getPattern());

                trigger(new SearchMessage.Response(self.getAddress(), event.getVodSource(), event.getTimeoutId(), event.getRequestId(), 0, 0, (IndexEntry[])result.toArray()), networkPort);
			} catch (IOException ex) {
				java.util.logging.Logger.getLogger(Search.class.getName()).log(Level.SEVERE, null,
						ex);
			} catch (IllegalSearchString illegalSearchString) {
                illegalSearchString.printStackTrace();
            }
        }
	};

	/**
	 * Add the response to the search index store.
	 */
	Handler<SearchMessage.Response> handleSearchResponse = new Handler<SearchMessage.Response>() {
		@Override
		public void handle(SearchMessage.Response event) {
			if (searchRequest == null
					|| event.getRequestId().equals(searchRequest.getSearchId()) == false) {
				return;
			}

			addSearchResponse(event.getResults());
		}
	};

	/**
	 * Answer a search request if the timeout occurred before all answers were
	 * collected.
	 */
	Handler<SearchTimeout> handleSearchTimeout = new Handler<SearchTimeout>() {
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
	Handler<ReplicationTimeout> handleReplicationTimeout = new Handler<ReplicationTimeout>() {
		@Override
		public void handle(ReplicationTimeout event) {
			// TODO We could send a message to the client here that we are
			// unsure if it worked. The client can then search the entry later
			// to check this and insert it again if necessary.

			// Garbage collect entry
			replicationRequests.remove(event.getRequestId());
		}
	};

	/**
	 * No acknowledgment for a issued {@link AddIndexEntry} request was received
	 * in time. Try to add the entry again or respons with failure to the web
	 * client.
	 */
	Handler<AddRequestTimeout> handleAddRequestTimeout = new Handler<AddRequestTimeout>() {
		@Override
		public void handle(AddRequestTimeout event) {
			if (event.reachedRetryLimit()) {
				// TODO inform the user
			} else {
				event.incrementTries();
				addEntryGlobal(event.getEntry(), (UUID)event.getTimeoutId());

				ScheduleTimeout rst = new ScheduleTimeout(config.getAddTimeout());
				rst.setTimeoutEvent(event);
				trigger(rst, timerPort);
			}
		}
	};

	/**
	 * Periodically garbage collect the data structure used to identify
	 * duplicated {@link AddIndexEntry} requests.
	 */
	Handler<RecentRequestsGcTimeout> handleRecentRequestsGcTimeout = new Handler<RecentRequestsGcTimeout>() {
		@Override
		public void handle(RecentRequestsGcTimeout event) {
			long referenceTime = System.currentTimeMillis();

			ArrayList<UUID> removeList = new ArrayList<UUID>();
			for (UUID uuid : recentRequests.keySet()) {
				if (referenceTime - recentRequests.get(uuid) > config
						.getRecentRequestsGcInterval()) {
					removeList.add(uuid);
				}
			}

			for (UUID uuid : removeList) {
				recentRequests.remove(uuid);
			}
		}
	};

	/**
	 * The entry for a detected gap was not added in time. Ask the leader to
	 * start the gap detection process.
	 */
	Handler<GapTimeout> handleGapTimeout = new Handler<GapTimeout>() {
		@Override
		public void handle(GapTimeout event) {
			try {
				if (entryExists(event.getId()) == false) {
					trigger(new GapCheck(self.getAddress(), event.getId()), routedEventsPort);
				}
			} catch (IOException e) {
				java.util.logging.Logger.getLogger(Search.class.getName()).log(Level.SEVERE, null,
						e);
			}
		}
	};

	/**
	 * In the role of the leader, handle a gap suspicion from a client. Start a
	 * random walk and search for the suspected entry.
	 */
	Handler<GapCheck> handleGapCheck = new Handler<GapCheck>() {
		@Override
		public void handle(GapCheck event) {
			// Don't start multiple detections for the same id
			if (gapDetections.containsKey(event.getId())) {
				return;
			}

			TreeSet<VodDescriptor> bucket = routingTable.get(event.getId()
					% config.getNumPartitions());
			if (bucket == null) {
				return;
			}

			for (VodDescriptor descriptor : bucket) {
				gapDetections.put(event.getId(), GapStatus.UNDECIDED);

                trigger(new GapDetectionMessage.Request(self.getAddress(), descriptor.getVodAddress(), UUID.nextUUID(), event.getId()), networkPort);
			}

			ScheduleTimeout rst = new ScheduleTimeout(config.getGapDetectionTimeout());
			rst.setTimeoutEvent(new GapDetectionTimeout(rst, event.getId()));
			trigger(rst, timerPort);
		}
	};

	/**
	 * Send a search request for a given search pattern to one node in each
	 * partition except the local partition.
	 *
	 * @param pattern
	 *            the search pattern
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

            trigger(new SearchMessage.Request(self.getAddress(),((PeerDescriptor) bucket.toArray()[n]).getAddress(), searchRequest.getTimeoutId(), searchRequest.getSearchId(), pattern), networkPort);
			searchRequest.incrementNodesQueried();
			i++;
		}

		ScheduleTimeout rst = new ScheduleTimeout(config.getQueryTimeout());
		rst.setTimeoutEvent(new SearchTimeout(rst));
		searchRequest.setTimeoutId((UUID)rst.getTimeoutEvent().getTimeoutId());
		trigger(rst, timerPort);

		// Add result form local partition
		try {
			ArrayList<IndexEntry> result = searchLocal(pattern);
			searchRequest.incrementNodesQueried();
			addSearchResponse((IndexEntry[])result.toArray());
		} catch (IOException e) {
			java.util.logging.Logger.getLogger(Search.class.getName()).log(Level.SEVERE, null, e);
		}
	}

	/**
	 * Present the result to the user.
	 */
	private void answerSearchRequest() {
		// TODO inform the user
	}

	/**
	 * Add a new {link {@link IndexEntry} to the system and schedule a timeout
	 * to wait for the acknowledgment.
	 * 
	 * @param entry
	 *            the {@link IndexEntry} to be added
	 */
	private void addEntryGlobal(IndexEntry entry) {
		// Limit the time to wait for responses and answer the web request
		ScheduleTimeout rst = new ScheduleTimeout(config.getAddTimeout());
		rst.setTimeoutEvent(new AddRequestTimeout(rst, config.getRetryCount(), entry));
		trigger(rst, timerPort);

		addEntryGlobal(entry, (UUID)rst.getTimeoutEvent().getTimeoutId());
	}

	/**
	 * Add a new {link {@link IndexEntry} to the system.
	 * 
	 * @param entry
	 *            the {@link IndexEntry} to be added
	 */
	private void addEntryGlobal(IndexEntry entry, UUID requestId) {
        System.out.println(self.getId() + " starts adding entry " + entry.getFileName());
		trigger(new AddIndexEntry(self.getAddress(), requestId, entry), routedEventsPort);
	}

	/**
	 * Add a new {link {@link IndexEntry} to the local Lucene index.
	 * 
	 * @param indexEntry
	 *            the {@link IndexEntry} to be added
	 * @throws IOException
	 *             if the Lucene index fails to store the entry
	 */
	private void addEntryLocal(IndexEntry indexEntry) throws IOException {
		if (indexEntry.getId() < oldestMissingIndexValue
				|| existingEntries.contains(indexEntry.getId())) {
			return;
		}

		addIndexEntry(index, indexEntry);
		Snapshot.incNumIndexEntries(self.getAddress().getPeerAddress());

		// Cancel gap detection timeouts for the given index
		UUID timeoutId = gapTimeouts.get(indexEntry.getId());
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
				rst.setTimeoutEvent(new GapTimeout(rst, i));
				gapTimeouts.put(indexEntry.getId(), (UUID)rst.getTimeoutEvent().getTimeoutId());
				trigger(rst, timerPort);
			}
		}
	}

	/**
	 * Query the Lucene index storing search request answers from different
	 * partition with the original search pattern to get the best results of all
	 * partitions.
	 * 
	 * @param sb
	 *            the string builder used to append the results
	 * @param pattern
	 *            the original search pattern sent by the client
	 * @return the string builder handed as a parameter which includes the
	 *         results
	 * @throws IOException
	 *             In case IOExceptions occurred in Lucene
	 */
	private String search(StringBuilder sb, SearchPattern pattern) throws IOException {
		IndexSearcher searcher = null;
		IndexReader reader = null;
		try {
			reader = DirectoryReader.open(searchIndex);
			searcher = new IndexSearcher(reader);
		} catch (IOException ex) {
			java.util.logging.Logger.getLogger(Search.class.getName()).log(Level.SEVERE, null, ex);
			System.exit(-1);
		}

		TopScoreDocCollector collector = TopScoreDocCollector.create(
				config.getHitsPerQuery(), true);
		searcher.search(pattern.getQuery(), collector);
		ScoreDoc[] hits = collector.topDocs().scoreDocs;

		// display results
		sb.append("Found ").append(hits.length).append(" entries.<ul>");
		for (int i = 0; i < hits.length; ++i) {
			int docId = hits[i].doc;
			Document d = searcher.doc(docId);
			sb.append("<tr><td>").append(i + 1).append("</td><td>")
					.append(d.get(IndexEntry.FILE_NAME)).append(".</td><td>")
					.append(d.get(IndexEntry.URL)).append("</td></tr>");
		}
		sb.append("</ul>");

		// reader can only be closed when there
		// is no need to access the documents any more.
		reader.close();
		return sb.toString();
	}

	/**
	 * Retrieve all indexes with ids in the given range from the local index
	 * store.
	 * 
	 * @param min
	 *            the inclusive minimum of the range
	 * @param max
	 *            the inclusive maximum of the range
	 * @param limit
	 *            the maximal amount of entries to be returned
	 * @return a list of the entries found
	 * @throws IOException
	 *             if Lucene errors occur
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
	private long getCurrentInsertionId() {
		lastInsertionId++;
//		trigger(new IndexDisseminationEvent(lastInsertionId), indexRoutingPort);
		return lastInsertionId;
	}

	/**
	 * Check if an entry with the given id exists in the local index store.
	 * 
	 * @param id
	 *            the id of the entry
	 * @return true if an entry with the given id exists
	 * @throws IOException
	 *             if Lucene errors occur
	 */
	private boolean entryExists(long id) throws IOException {
		IndexEntry indexEntry = findById(id);
		return indexEntry != null ? true : false;
	}

	/**
	 * Find an entry for the given id in the local index store.
	 * 
	 * @param id
	 *            the id of the entry
	 * @return the entry if found or null if non-existing
	 * @throws IOException
	 *             if Lucene errors occur
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
	 * @param entries
	 *            the entries to be added
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
	 * Query the local index store for a given query string.
	 * 
	 * @return a list of matching entries
	 * @throws IOException
	 *             if Lucene errors occur
	 */
	private ArrayList<IndexEntry> searchLocal(SearchPattern pattern) throws IOException {
		IndexReader reader = null;
		try {
			reader = DirectoryReader.open(index);
			IndexSearcher searcher = new IndexSearcher(reader);
			TopScoreDocCollector collector = TopScoreDocCollector.create(
					config.getHitsPerQuery(), true);
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
	 * @param writer
	 *            the writer used to add the {@link IndexEntry}
	 * @param entry
	 *            the {@link IndexEntry} to be added
	 * @throws IOException
	 *             in case the adding operation failed
	 */
	private void addIndexEntry(IndexWriter writer, IndexEntry entry) throws IOException {
		Document doc = new Document();
		doc.add(new LongField(IndexEntry.ID, entry.getId(), Field.Store.YES));
		doc.add(new StoredField(IndexEntry.URL, entry.getUrl()));
		doc.add(new TextField(IndexEntry.FILE_NAME, entry.getFileName(), Field.Store.YES));
		doc.add(new IntField(IndexEntry.CATEGORY, entry.getCategory().ordinal(), Field.Store.YES));
		doc.add(new TextField(IndexEntry.DESCRIPTION, entry.getDescription(), Field.Store.YES));
		doc.add(new StoredField(IndexEntry.HASH, entry.getHash()));
		doc.add(new StringField(IndexEntry.LEADER_ID, entry.getLeaderId(), Field.Store.YES));

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
	 * @param index
	 *            the directory to which the given {@link IndexEntry} should be
	 *            added
	 * @param entry
	 *            the {@link IndexEntry} to be added
	 * @throws IOException
	 *             in case the adding operation failed
	 */
	private void addIndexEntry(Directory index, IndexEntry entry) throws IOException {
		IndexWriter writer = new IndexWriter(index, indexWriterConfig);
		addIndexEntry(writer, entry);
		writer.close();
	}

	/**
	 * Add the given {@link IndexEntry}s to the given Lucene directory
	 * 
	 * @param index
	 *            the directory to which the given entries should be added
	 * @param entries
	 *            a collection of index entries to be added
	 * @throws IOException
	 *             in case the adding operation failed
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
	 * @param d
	 *            the document to create an {@link IndexEntry} from
	 * @return an {@link IndexEntry} representing the given document
	 */
	private IndexEntry createIndexEntry(Document d) {
		IndexEntry entry = new IndexEntry(Long.valueOf(d.get(IndexEntry.ID)),
                d.get(IndexEntry.URL), d.get(IndexEntry.FILE_NAME),
                IndexEntry.Category.values()[Integer.valueOf(d.get(IndexEntry.CATEGORY))],
				d.get(IndexEntry.DESCRIPTION), d.get(IndexEntry.HASH), d.get(IndexEntry.LEADER_ID));

		return entry;
	}
}
