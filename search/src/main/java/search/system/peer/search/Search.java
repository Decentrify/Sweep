package search.system.peer.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.logging.Level;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
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
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.CancelTimeout;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.kompics.timer.Timer;
import se.sics.kompics.web.Web;
import se.sics.kompics.web.WebRequest;
import se.sics.kompics.web.WebResponse;
import search.system.peer.IndexPort;
import search.system.peer.IndexPort.AddIndexSimulated;
import search.system.peer.search.IndexExchangeMessages.IndexUpdateRequest;
import search.system.peer.search.IndexExchangeMessages.IndexUpdateResponse;
import search.system.peer.search.LeaderResponse.IndexEntryAdded;
import search.system.peer.search.ReplicationMessage.Replicate;
import search.system.peer.search.ReplicationMessage.ReplicationConfirmation;
import search.system.peer.search.SearchMessage.SearchRequest;
import search.system.peer.search.SearchMessage.SearchResponse;
import search.system.peer.search.Timeouts.AddRequestTimeout;
import search.system.peer.search.Timeouts.GapDetectionTimeout;
import search.system.peer.search.Timeouts.GapTimeout;
import search.system.peer.search.Timeouts.RecentRequestsGcTimeout;
import search.system.peer.search.Timeouts.ReplicationTimeout;
import search.system.peer.search.Timeouts.SearchTimeout;
import tman.system.peer.tman.GapDetectionMessage.GapDetectionRequest;
import tman.system.peer.tman.GapDetectionMessage.GapDetectionResponse;
import tman.system.peer.tman.IndexRoutingPort;
import tman.system.peer.tman.IndexRoutingPort.IndexDisseminationEvent;
import tman.system.peer.tman.IndexRoutingPort.IndexRequestEvent;
import tman.system.peer.tman.IndexRoutingPort.IndexResponseMessage;
import tman.system.peer.tman.IndexRoutingPort.StartIndexRequestEvent;
import tman.system.peer.tman.LeaderRequest.AddIndexEntry;
import tman.system.peer.tman.LeaderRequest.GapCheck;
import tman.system.peer.tman.RoutedEventsPort;
import tman.system.peer.tman.TMan;

import common.configuration.SearchConfiguration;
import common.entities.IndexEntry;
import common.peer.PeerDescriptor;
import common.snapshot.Snapshot;

import cyclon.CyclonPort;
import cyclon.CyclonSample;
import cyclon.CyclonSamplePort;

/**
 * This class handles the storing, adding and searching for indexes. It acts in
 * two different modes depending on if it the executing node was elected leader
 * or not, although it doesn't know about the leader status. {@link TMan} knows
 * about the leader status and only forwards according messages to this
 * component in case the local node is elected leader.
 * 
 * {@link IndexEntry}s are spread via gossiping using the Cyclon samples stored
 * in the routing tables for the partition of the local node.
 */
public final class Search extends ComponentDefinition {
	Positive<IndexPort> indexPort = positive(IndexPort.class);
	Positive<Network> networkPort = positive(Network.class);
	Positive<Timer> timerPort = positive(Timer.class);
	Negative<Web> webPort = negative(Web.class);
	Positive<CyclonSamplePort> cyclonSamplePort = positive(CyclonSamplePort.class);
	Positive<RoutedEventsPort> routedEventsPort = positive(RoutedEventsPort.class);
	Positive<CyclonPort> partitionCyclonPort = positive(CyclonPort.class);
	Positive<IndexRoutingPort> indexRoutingPort = positive(IndexRoutingPort.class);

	private static final Logger logger = LoggerFactory.getLogger(Search.class);
	private Address self;
	private SearchConfiguration searchConfiguration;
	// The last smallest missing index number.
	private int oldestMissingIndexValue;
	// Set of existing entries higher than the oldestMissingIndexValue
	private SortedSet<Integer> existingEntries;
	// The last id used for adding new entries in case this node is the leader
	private int lastInsertionId;
	// Open web requests from web clients
	private Map<UUID, WebRequest> openRequests;
	// Data structure to keep track of acknowledgments for newly added indexes
	private Map<UUID, ReplicationCount> replicationRequests;
	private Random random;
	// The number of the local partition
	private int partition;
	// Structure that maps index ids to UUIDs of open gap timeouts
	private Map<Integer, UUID> gapTimeouts;

	/**
	 * Enum describing the status of the gap detection process.
	 */
	private enum GapStatus {
		UNDECIDED, TRUE, FALSE
	}

	// Maps index ids for currently issued gap detections to their status
	private Map<Integer, GapStatus> gapDetections;

	// Set of recent add requests to avoid duplication
	private Map<UUID, Long> recentRequests;

	// Apache Lucene used for searching
	private StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_42);
	private Directory index = new RAMDirectory();
	private IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_42, analyzer);

	// Lucene variables used to store and search in collected answers
	private LocalSearchRequest searchRequest;
	private Directory searchIndex;

	// When you partition the index you need to find new nodes
	// This is a routing table maintaining a list of pairs in each partition.
	private Map<Integer, TreeSet<PeerDescriptor>> routingTable;
	Comparator<PeerDescriptor> peerAgeComparator = new Comparator<PeerDescriptor>() {
		@Override
		public int compare(PeerDescriptor t0, PeerDescriptor t1) {
			if (t0.getAddress().equals(t1.getAddress())) {
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
		subscribe(handleWebRequest, webPort);
		subscribe(handleCyclonSample, cyclonSamplePort);
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
		subscribe(handleGapDetectionRequest, networkPort);
		subscribe(handleGapDetectionResponse, networkPort);
		subscribe(handleGapDetectionTimeout, timerPort);
		subscribe(handleRecentRequestsGcTimeout, timerPort);
		subscribe(handleIndexUpdate, indexRoutingPort);
		subscribe(handleStartIndexRequest, indexRoutingPort);
		subscribe(handleIndexRequest, indexRoutingPort);
	}

	/**
	 * Initialize the component.
	 */
	Handler<SearchInit> handleInit = new Handler<SearchInit>() {
		public void handle(SearchInit init) {
			self = init.getSelf();
			searchConfiguration = init.getConfiguration();
			routingTable = new HashMap<Integer, TreeSet<PeerDescriptor>>(
					searchConfiguration.getNumPartitions());
			lastInsertionId = -1;
			openRequests = new HashMap<UUID, WebRequest>();
			replicationRequests = new HashMap<UUID, ReplicationCount>();
			random = new Random(init.getConfiguration().getSeed());
			partition = self.getId() % searchConfiguration.getNumPartitions();
			oldestMissingIndexValue = partition;
			existingEntries = new TreeSet<Integer>();
			gapTimeouts = new HashMap<Integer, UUID>();
			gapDetections = new HashMap<Integer, Search.GapStatus>();

			recentRequests = new HashMap<UUID, Long>();
			// Garbage collect the data structure
			SchedulePeriodicTimeout rst = new SchedulePeriodicTimeout(
					searchConfiguration.getRecentRequestsGcInterval(),
					searchConfiguration.getRecentRequestsGcInterval());
			rst.setTimeoutEvent(new RecentRequestsGcTimeout(rst));
			trigger(rst, timerPort);

			// Can't open the index before committing a writer once
			IndexWriter writer;
			try {
				writer = new IndexWriter(index, config);
				writer.commit();
				writer.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	};

	/**
	 * Parse the GET request of a web request and decide what to do.
	 */
	Handler<WebRequest> handleWebRequest = new Handler<WebRequest>() {
		public void handle(WebRequest event) {
			String[] args = event.getTarget().split("-");
			logger.debug("Handling Webpage Request");
			if (args[0].compareToIgnoreCase("search") == 0 && args.length == 2) {
				startSearch(event, args[1]);
			} else if (args[0].compareToIgnoreCase("add") == 0 && args.length == 3) {
				addEntryGlobal(new IndexEntry(args[1], args[2]), event);
			} else {
				trigger(new WebResponse("Invalid request", event, 1, 1), webPort);
			}
		}
	};

	/**
	 * Handle samples from Cyclon. Use them to update the routing tables and
	 * issue an index exchange with another node.
	 */
	Handler<CyclonSample> handleCyclonSample = new Handler<CyclonSample>() {
		@Override
		public void handle(CyclonSample event) {
			// receive a new list of neighbors
			ArrayList<Address> peers = event.getSample();
			if (peers.isEmpty()) {
				return;
			}

			// update routing tables
			for (Address p : event.getSample()) {
				int samplePartition = p.getId() % searchConfiguration.getNumPartitions();
				TreeSet<PeerDescriptor> nodes = routingTable.get(samplePartition);
				if (nodes == null) {
					nodes = new TreeSet<PeerDescriptor>(peerAgeComparator);
					routingTable.put(samplePartition, nodes);
				}

				// Increment age
				for (PeerDescriptor peer : nodes) {
					peer.incrementAndGetAge();
				}

				// Note - this might replace an existing entry
				nodes.add(new PeerDescriptor(p));
				// keep the freshest descriptors in this partition
				while (nodes.size() > searchConfiguration.getMaxNumRoutingEntries()) {
					nodes.pollLast();
				}
			}

			// Exchange index with one sample from our partition
			TreeSet<PeerDescriptor> bucket = routingTable.get(partition);
			if (bucket != null) {
				int n = random.nextInt(bucket.size());
				trigger(new IndexUpdateRequest(self,
						((PeerDescriptor) bucket.toArray()[n]).getAddress(),
						oldestMissingIndexValue,
						existingEntries.toArray(new Integer[existingEntries.size()])), networkPort);
			}
		}
	};

	/**
	 * Add index entries for the simulator.
	 */
	Handler<AddIndexSimulated> handleAddIndexSimulated = new Handler<AddIndexSimulated>() {
		@Override
		public void handle(AddIndexSimulated event) {
			// logger.info(self.getId() + " - adding index entry: {}-{}",
			// event.getEntry().getTitle(),
			// event.getEntry().getMagneticLink());
			addEntryGlobal(event.getEntry(), UUID.randomUUID());
		}
	};

	/**
	 * Add all entries received from another node to the local index store.
	 */
	Handler<IndexUpdateResponse> handleIndexUpdateResponse = new Handler<IndexUpdateResponse>() {
		@Override
		public void handle(IndexUpdateResponse event) {
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
	Handler<IndexUpdateRequest> handleIndexUpdateRequest = new Handler<IndexUpdateRequest>() {
		@Override
		public void handle(IndexUpdateRequest event) {
			try {
				List<IndexEntry> indexEntries = new ArrayList<IndexEntry>();

				// Search for entries the inquirer is missing
				Integer lastId = event.getOldestMissingIndexValue();
				for (Integer i : event.getExistingEntries()) {
					indexEntries.addAll(findIdRange(lastId,
							i - searchConfiguration.getNumPartitions(),
							searchConfiguration.getMaxExchangeCount() - indexEntries.size()));
					lastId = i + searchConfiguration.getNumPartitions();

					if (indexEntries.size() >= searchConfiguration.getMaxExchangeCount()) {
						break;
					}
				}

				// In case there is some space left search for more
				if (indexEntries.size() < searchConfiguration.getMaxExchangeCount()) {
					indexEntries.addAll(findIdRange(lastId, Integer.MAX_VALUE,
							searchConfiguration.getMaxExchangeCount() - indexEntries.size()));
				}

				if (indexEntries.isEmpty()) {
					return;
				}

				trigger(new IndexUpdateResponse(self, event.getSource(), indexEntries), networkPort);
			} catch (IOException e) {
				logger.error(self.getId() + " " + e.getMessage());
			}
		}

	};

	/**
	 * Handler executed in the role of the leader. Create a new id and search
	 * for a the according bucket in the routing table. If it does not include
	 * enough nodes to satisfy the replication requirements then create a new id
	 * and try again. Send a {@link Replicate} request to a number of nodes as
	 * specified in the config file and schedule a timeout to wait for
	 * responses. The adding operation will be acknowledged if either all nodes
	 * responded to the {@link Replicate} request or the timeout occurred and
	 * enough nodes, as specified in the config, responded.
	 */
	Handler<AddIndexEntry> handleAddIndexEntry = new Handler<AddIndexEntry>() {
		@Override
		public void handle(AddIndexEntry event) {
			if (recentRequests.containsKey(event.getUuid())) {
				return;
			}
			recentRequests.put(event.getUuid(), System.currentTimeMillis());

			try {
				if (routingTable.isEmpty()) {
					// There's nothing we can do here
					return;
				}

				// Search the next id and a non-empty bucket an place the entry
				// there
				IndexEntry newEntry = event.getIndexEntry();
				int id, entryPartition;
				TreeSet<PeerDescriptor> bucket;
				int i = routingTable.size();
				do {
					id = getCurrentInsertionId();

					entryPartition = id % searchConfiguration.getNumPartitions();
					bucket = routingTable.get(entryPartition);
					i--;
				} while ((bucket == null || searchConfiguration.getReplicationMinimum() > bucket
						.size()) && i > 0);

				// There is nothing we can do
				if (bucket == null || searchConfiguration.getReplicationMinimum() > bucket.size()) {
					return;
				}

				newEntry.setId(id);
				if (entryPartition == partition) {
					addEntryLocal(newEntry);
				}

				replicationRequests.put(event.getUuid(), new ReplicationCount(event.getSource(),
						searchConfiguration.getReplicationMinimum()));

				i = bucket.size() > searchConfiguration.getReplicationMaximum() ? searchConfiguration
						.getReplicationMaximum() : bucket.size();
				for (PeerDescriptor peer : bucket) {
					if (i == 0) {
						break;
					}
					trigger(new Replicate(self, peer.getAddress(), newEntry, event.getUuid()),
							networkPort);
					i--;
				}

				ScheduleTimeout rst = new ScheduleTimeout(
						searchConfiguration.getReplicationTimeout());
				rst.setTimeoutEvent(new ReplicationTimeout(rst, event.getUuid()));
				trigger(rst, timerPort);

				Snapshot.setLastId(id);
			} catch (IOException e) {
				logger.error(self.getId() + " " + e.getMessage());
			}
		}
	};

	/**
	 * Respond to the web client after receiving an acknowledgment for and
	 * adding operation.
	 */
	Handler<IndexEntryAdded> handleIndexEntryAdded = new Handler<IndexEntryAdded>() {
		@Override
		public void handle(IndexEntryAdded event) {
			WebRequest webRequest = openRequests.get(event.getUuid());

			CancelTimeout ct = new CancelTimeout(event.getUuid());
			trigger(ct, timerPort);

			if (webRequest != null) {
				StringBuilder sb = new StringBuilder("<!DOCTYPE html PUBLIC \"-//W3C");
				sb.append("//DTD XHTML 1.0 Transitional//EN\" \"http://www.w3.org/TR");
				sb.append("/xhtml1/DTD/xhtml1-transitional.dtd\"><html xmlns=\"http:");
				sb.append("//www.w3.org/1999/xhtml\"><head><meta http-equiv=\"Conten");
				sb.append("t-Type\" content=\"text/html; charset=utf-8\" />");
				sb.append("<title>Adding an Entry</title>");
				sb.append("<style type=\"text/css\"><!--.style2 {font-family: ");
				sb.append("Arial, Helvetica, sans-serif; color: #0099FF;}--></style>");
				sb.append("</head><body><h2 align=\"center\" class=\"style2\">");
				sb.append("ID2210 Uploaded Entry</h2><br>");
				sb.append("Index has been added");
				sb.append("</body></html>");

				trigger(new WebResponse(sb.toString(), webRequest, 1, 1), webPort);
				openRequests.remove(event.getUuid());
			}
		}
	};

	/**
	 * When receiving a replicate messsage from the leader, add the entry to the
	 * local store and send an acknowledgment.
	 */
	Handler<Replicate> handleReplicate = new Handler<Replicate>() {
		@Override
		public void handle(Replicate event) {
			try {
				addEntryLocal(event.getIndexEntry());
				trigger(new ReplicationConfirmation(self, event.getSource(), event.getUuid()),
						networkPort);
			} catch (IOException e) {
				logger.error(self.getId() + " " + e.getMessage());
			}
		}
	};

	/**
	 * As the leader, add an {@link ReplicationConfirmation} to the according
	 * request and issue the response if the replication constraints were
	 * satisfied.
	 */
	Handler<ReplicationConfirmation> handleReplicationConfirmation = new Handler<ReplicationConfirmation>() {
		@Override
		public void handle(ReplicationConfirmation event) {
			ReplicationCount replicationCount = replicationRequests.get(event.getUuid());
			if (replicationCount != null && replicationCount.incrementAndCheckReceived()) {
				trigger(new IndexEntryAdded(self, replicationCount.getSource(), event.getUuid()),
						networkPort);
				replicationRequests.remove(event.getUuid());
			}
		}
	};

	/**
	 * Query the local store with the given query string and send the response
	 * back to the inquirer.
	 */
	Handler<SearchRequest> handleSearchRequest = new Handler<SearchRequest>() {
		@Override
		public void handle(SearchRequest event) {
			try {
				ArrayList<IndexEntry> result = searchLocal(event.getQuery());
				trigger(new SearchResponse(self, event.getSource(), event.getRequestId(), result),
						networkPort);
			} catch (ParseException ex) {
				java.util.logging.Logger.getLogger(Search.class.getName()).log(Level.SEVERE, null,
						ex);
			} catch (IOException ex) {
				java.util.logging.Logger.getLogger(Search.class.getName()).log(Level.SEVERE, null,
						ex);
			}
		}
	};

	/**
	 * Add the response to the search index store.
	 */
	Handler<SearchResponse> handleSearchResponse = new Handler<SearchResponse>() {
		@Override
		public void handle(SearchResponse event) {
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
				WebRequest webRequest = openRequests.remove(event.getTimeoutId());
				// Somehow all peers get the timeout scheduled by one
				if (webRequest != null) {
					trigger(new WebResponse("Insert failed", webRequest, 1, 1), webPort);
				}
			} else {
				event.incrementTries();
				addEntryGlobal(event.getEntry(), event.getTimeoutId());

				ScheduleTimeout rst = new ScheduleTimeout(searchConfiguration.getAddTimeout());
				rst.setTimeoutEvent(event);
				trigger(rst, timerPort);
			}
		}
	};

	/**
	 * A handler that will update the current least insertion ID in case the one
	 * in the message is bigger. This handler is called when either it is
	 * following a leader that broadcasts its last index ID, or when a new
	 * leader is searching for the highest index ID among other nodes
	 */
	Handler<IndexDisseminationEvent> handleIndexUpdate = new Handler<IndexRoutingPort.IndexDisseminationEvent>() {
		@Override
		public void handle(IndexDisseminationEvent event) {
			if (event.getIndex() > lastInsertionId) {
				lastInsertionId = event.getIndex();
			}
		}
	};

	/**
	 * This handler is called when a new leader should start looking for the
	 * biggest index ID among its peers
	 */
	Handler<StartIndexRequestEvent> handleStartIndexRequest = new Handler<StartIndexRequestEvent>() {
		@Override
		public void handle(StartIndexRequestEvent event) {
			trigger(new IndexRequestEvent(lastInsertionId, event.getMessageID(), self),
					indexRoutingPort);
		}
	};

	/**
	 * This handler respond to new leaders who are searching for the highest
	 * index ID by returning their own index ID
	 */
	Handler<IndexRequestEvent> handleIndexRequest = new Handler<IndexRequestEvent>() {
		@Override
		public void handle(IndexRequestEvent event) {
			trigger(new IndexResponseMessage(lastInsertionId, event.getMessageId(), self,
					event.getLeaderAddress()), networkPort);
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
				if (referenceTime - recentRequests.get(uuid) > searchConfiguration
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
					trigger(new GapCheck(event.getId()), routedEventsPort);
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

			for (PeerDescriptor descriptor : routingTable.get(event.getId()
					% searchConfiguration.getNumPartitions())) {
				gapDetections.put(event.getId(), GapStatus.UNDECIDED);
				trigger(new GapDetectionRequest(self, descriptor.getAddress(), event.getId(),
						searchConfiguration.getGapDetectionTtl()), networkPort);
			}

			ScheduleTimeout rst = new ScheduleTimeout(searchConfiguration.getGapDetectionTimeout());
			rst.setTimeoutEvent(new GapDetectionTimeout(rst, event.getId()));
			trigger(rst, timerPort);
		}
	};

	/**
	 * Answer a gap detection request from the leader and forward it to a random
	 * node if the TTL is not expired.
	 */
	Handler<GapDetectionRequest> handleGapDetectionRequest = new Handler<GapDetectionRequest>() {
		@Override
		public void handle(GapDetectionRequest event) {
			try {
				trigger(new GapDetectionResponse(self, event.getSource(), event.getId(),
						!entryExists(event.getId())), networkPort);

				event.decrementTtl();
				if (event.getTtl() == 0) {
					TreeSet<PeerDescriptor> bucket = routingTable.get(event.getId()
							% searchConfiguration.getNumPartitions());

					if (bucket != null) {
						int n = random.nextInt(bucket.size());
						event.setDestination(((PeerDescriptor) bucket.toArray()[n]).getAddress());
						trigger(event, networkPort);
					}
				}
			} catch (IOException e) {
				java.util.logging.Logger.getLogger(Search.class.getName()).log(Level.SEVERE, null,
						e);
			}
		}
	};

	/**
	 * As leader, collect answers from a gap detection random walk.
	 */
	Handler<GapDetectionResponse> handleGapDetectionResponse = new Handler<GapDetectionResponse>() {
		@Override
		public void handle(GapDetectionResponse event) {
			if (gapDetections.containsKey(event.getId())) {
				if (gapDetections.get(event.getId()) == GapStatus.FALSE) {
					return;
				}

				if (event.isGap() == false) {
					gapDetections.put(event.getId(), GapStatus.FALSE);
				} else {
					gapDetections.put(event.getId(), GapStatus.TRUE);
				}
			}
		}
	};

	/**
	 * As the leader, evaluate the {@link GapDetectionResponse}s collected and
	 * create a tombstone if necessary.
	 */
	Handler<GapDetectionTimeout> handleGapDetectionTimeout = new Handler<GapDetectionTimeout>() {
		@Override
		public void handle(GapDetectionTimeout event) {
			if (gapDetections.remove(event.getId()) == GapStatus.TRUE) {
				int entryPartition = event.getId() % searchConfiguration.getMaxNumRoutingEntries();
				TreeSet<PeerDescriptor> bucket = routingTable.get(entryPartition);

				Snapshot.addGap(event.getId());

				if (bucket == null) {
					return;
				}

				// An entry with an empty title is a tombstone
				IndexEntry tombstone = new IndexEntry("", "", event.getId());
				if (entryPartition == partition) {
					try {
						addEntryLocal(tombstone);
					} catch (IOException e) {
						java.util.logging.Logger.getLogger(Search.class.getName()).log(
								Level.SEVERE, null, e);
					}
				}

				for (PeerDescriptor peer : bucket) {
					trigger(new Replicate(self, peer.getAddress(), tombstone, UUID.randomUUID()),
							networkPort);
				}
			}
		}
	};

	/**
	 * Send a search request for a given query to one node in each partition
	 * except the local partition.
	 * 
	 * @param event
	 *            the web event of the client that issued the search
	 * @param query
	 *            the query string
	 */
	private void startSearch(WebRequest event, String query) {
		searchRequest = new LocalSearchRequest(event, query);
		searchIndex = new RAMDirectory();

		// Can't open the index before committing a writer once
		IndexWriter writer;
		try {
			writer = new IndexWriter(searchIndex, config);
			writer.commit();
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		int i = 0;
		for (SortedSet<PeerDescriptor> bucket : routingTable.values()) {
			// Skip local partition
			if (i == partition) {
				i++;
				continue;
			}

			int n = random.nextInt(bucket.size());
			trigger(new SearchRequest(self, ((PeerDescriptor) bucket.toArray()[n]).getAddress(),
					searchRequest.getSearchId(), query), networkPort);
			searchRequest.incrementNodesQueried();
			i++;
		}

		ScheduleTimeout rst = new ScheduleTimeout(searchConfiguration.getSearchTimeout());
		rst.setTimeoutEvent(new SearchTimeout(rst));
		searchRequest.setTimeoutId(rst.getTimeoutEvent().getTimeoutId());
		trigger(rst, timerPort);

		// Add result form local partition
		try {
			ArrayList<IndexEntry> result = searchLocal(query);
			searchRequest.incrementNodesQueried();
			addSearchResponse(result);
		} catch (ParseException e) {
			java.util.logging.Logger.getLogger(Search.class.getName()).log(Level.SEVERE, null, e);
		} catch (IOException e) {
			java.util.logging.Logger.getLogger(Search.class.getName()).log(Level.SEVERE, null, e);
		}
	}

	/**
	 * Create an html document including the search results for the current
	 * search request and sends a response back to the issuer.
	 */
	private void answerSearchRequest() {
		StringBuilder sb = new StringBuilder("<!DOCTYPE html PUBLIC \"-//W3C");
		sb.append("//DTD XHTML 1.0 Transitional//EN\" \"http://www.w3.org/TR");
		sb.append("/xhtml1/DTD/xhtml1-transitional.dtd\"><html xmlns=\"http:");
		sb.append("//www.w3.org/1999/xhtml\"><head><meta http-equiv=\"Conten");
		sb.append("t-Type\" content=\"text/html; charset=utf-8\" />");
		sb.append("<title>Kompics P2P Bootstrap Server</title>");
		sb.append("<style type=\"text/css\"><!--.style2 {font-family: ");
		sb.append("Arial, Helvetica, sans-serif; color: #0099FF;}--></style>");
		sb.append("</head><body><h2 align=\"center\" class=\"style2\">");
		sb.append("ID2210 (Decentralized Search for Piratebay)</h2><br>");
		sb.append("<table>");
		try {
			query(sb, searchRequest.getQuery());
		} catch (ParseException ex) {
			java.util.logging.Logger.getLogger(Search.class.getName()).log(Level.SEVERE, null, ex);
			sb.append(ex.getMessage());
		} catch (IOException ex) {
			java.util.logging.Logger.getLogger(Search.class.getName()).log(Level.SEVERE, null, ex);
			sb.append(ex.getMessage());
		}
		sb.append("</table>");
		sb.append("</body></html>");

		trigger(new WebResponse(sb.toString(), searchRequest.getWebRequest(), 1, 1), webPort);
		searchRequest = null;
	}

	/**
	 * Add a new {link {@link IndexEntry} to the system and schedule a timeout
	 * to wait for the acknowledgment.
	 * 
	 * @param entry
	 *            the {@link IndexEntry} to be added
	 * @param event
	 *            the web event of the client issuing the request
	 */
	private void addEntryGlobal(IndexEntry entry, WebRequest event) {
		// Limit the time to wait for responses and answer the web request
		ScheduleTimeout rst = new ScheduleTimeout(searchConfiguration.getAddTimeout());
		rst.setTimeoutEvent(new AddRequestTimeout(rst, searchConfiguration.getRetryCount(), entry));
		trigger(rst, timerPort);

		openRequests.put(rst.getTimeoutEvent().getTimeoutId(), event);
		addEntryGlobal(entry, rst.getTimeoutEvent().getTimeoutId());
	}

	/**
	 * Add a new {link {@link IndexEntry} to the system.
	 * 
	 * @param entry
	 *            the {@link IndexEntry} to be added
	 * @param event
	 *            the request id used to identify the request
	 */
	private void addEntryGlobal(IndexEntry entry, UUID requestId) {
		trigger(new AddIndexEntry(self, requestId, entry), routedEventsPort);
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

		IndexWriter w = new IndexWriter(index, config);
		Document doc = new Document();
		doc.add(new TextField("title", indexEntry.getTitle(), Field.Store.YES));
		doc.add(new TextField("magnetic", indexEntry.getMagneticLink(), Field.Store.YES));
		doc.add(new IntField("id", indexEntry.getId(), Field.Store.YES));
		w.addDocument(doc);
		w.close();
		Snapshot.incNumIndexEntries(self);

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
				oldestMissingIndexValue += searchConfiguration.getNumPartitions();
			} while (existingEntries.contains(oldestMissingIndexValue));
		} else if (indexEntry.getId() > oldestMissingIndexValue) {
			existingEntries.add(indexEntry.getId());

			// Suspect all missing entries less than the new as gaps
			for (int i = oldestMissingIndexValue; i < indexEntry.getId(); i = i
					+ searchConfiguration.getNumPartitions()) {
				if (gapTimeouts.containsKey(i)) {
					continue;
				}

				// This might be a gap so start a timeouts
				ScheduleTimeout rst = new ScheduleTimeout(searchConfiguration.getGapTimeout());
				rst.setTimeoutEvent(new GapTimeout(rst, i));
				gapTimeouts.put(indexEntry.getId(), rst.getTimeoutEvent().getTimeoutId());
				trigger(rst, timerPort);
			}
		}
	}

	/**
	 * Query the Lucene index storing search request answers from different
	 * partition with the original query to get the best results of all
	 * partitions.
	 * 
	 * @param sb
	 *            the string builder used to append the results
	 * @param querystr
	 *            the original query sent by the client
	 * @return the string builder handed as a parameter which includes the
	 *         results
	 * @throws ParseException
	 *             if the query could not be parsed by Lucene
	 * @throws IOException
	 *             In case IOExceptions occurred in Lucene
	 */
	private String query(StringBuilder sb, String querystr) throws ParseException, IOException {
		Query q = new QueryParser(Version.LUCENE_42, "title", analyzer).parse(querystr);
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
				searchConfiguration.getHitsPerQuery(), true);

		searcher.search(q, collector);
		ScoreDoc[] hits = collector.topDocs().scoreDocs;

		// display results
		sb.append("Found ").append(hits.length).append(" entries.<ul>");
		for (int i = 0; i < hits.length; ++i) {
			int docId = hits[i].doc;
			Document d = searcher.doc(docId);
			sb.append("<tr><td>").append(i + 1).append("</td><td>").append(d.get("title"))
					.append(".</td><td>").append(d.get("magnetic")).append("</td></tr>");
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
	private List<IndexEntry> findIdRange(int min, int max, int limit) throws IOException {
		IndexReader reader = null;
		try {
			reader = DirectoryReader.open(index);
			IndexSearcher searcher = new IndexSearcher(reader);

			Query query = NumericRangeQuery.newIntRange("id", min, max, true, true);
			TopDocs topDocs = searcher
					.search(query, limit, new Sort(new SortField("id", Type.INT)));
			ArrayList<IndexEntry> indexEntries = new ArrayList<IndexEntry>();
			for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
				Document d = searcher.doc(scoreDoc.doc);
				indexEntries.add(new IndexEntry(d.get("title"), d.get("magnetic"), Integer
						.valueOf(d.get("id"))));
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
	private int getCurrentInsertionId() {
		lastInsertionId++;
		trigger(new IndexDisseminationEvent(lastInsertionId), indexRoutingPort);
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
	private boolean entryExists(int id) throws IOException {
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
	private IndexEntry findById(int id) throws IOException {
		List<IndexEntry> indexEntries = findIdRange(id, id, 1);
		if (indexEntries.isEmpty()) {
			return null;
		}
		return indexEntries.get(0);
	}

	/**
	 * Add all entries from a {@link SearchResponse} to the search index.
	 * 
	 * @param entries
	 *            the entries to be added
	 */
	private void addSearchResponse(Collection<IndexEntry> entries) {
		IndexWriter writer = null;
		try {
			writer = new IndexWriter(searchIndex, config);
			for (IndexEntry indexEntry : entries) {
				Document doc = new Document();
				doc.add(new TextField("title", indexEntry.getTitle(), Field.Store.YES));
				doc.add(new TextField("magnetic", indexEntry.getMagneticLink(), Field.Store.YES));
				doc.add(new IntField("id", indexEntry.getId(), Field.Store.YES));
				writer.addDocument(doc);
			}
			writer.commit();
		} catch (IOException e) {
			java.util.logging.Logger.getLogger(Search.class.getName()).log(Level.SEVERE, null, e);
		} finally {
			if (writer != null) {
				try {
					writer.close();
				} catch (IOException e) {
				}
			}
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
	 * @param query
	 *            the query string
	 * @return a list of matching entries
	 * @throws ParseException
	 *             if Lucene errors occur
	 * @throws IOException
	 *             if Lucene errors occur
	 */
	private ArrayList<IndexEntry> searchLocal(String query) throws ParseException, IOException {
		IndexReader reader = null;
		try {
			Query q = new QueryParser(Version.LUCENE_42, "title", analyzer).parse(query);
			reader = DirectoryReader.open(index);
			IndexSearcher searcher = new IndexSearcher(reader);

			int hitsPerPage = 10;
			TopScoreDocCollector collector = TopScoreDocCollector.create(hitsPerPage, true);

			searcher.search(q, collector);
			ScoreDoc[] hits = collector.topDocs().scoreDocs;

			ArrayList<IndexEntry> result = new ArrayList<IndexEntry>();
			for (int i = 0; i < hits.length; ++i) {
				int docId = hits[i].doc;
				Document d = searcher.doc(docId);
				result.add(new IndexEntry(d.get("title"), d.get("magnetic"), Integer.valueOf(d
						.get("id"))));
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
}
