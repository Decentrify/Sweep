package common.snapshot;

import java.util.ArrayList;
import java.util.Collections;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListSet;

import se.sics.kompics.address.Address;

/**
 * Keep track of the system state for evaluation and debugging. Write the
 * information in a log file.
 */
public class Snapshot {
	private static SortedMap<Address, PeerInfo> peers = Collections
			.synchronizedSortedMap(new TreeMap<Address, PeerInfo>());
	private static int counter = 0;
	private static String FILENAME = "search.out";
	private static ConcurrentSkipListSet<Integer> detectedGaps = new ConcurrentSkipListSet<Integer>();
	private static int lastId = -1;
	private static ConcurrentSkipListSet<Integer> idDuplicates = new ConcurrentSkipListSet<Integer>();
	private static int entriesAdded = 0;
	private static ConcurrentSkipListSet<Address> oldLeaders = new ConcurrentSkipListSet<Address>();

	public static void init(int numOfStripes) {
		FileIO.write("", FILENAME);
	}

	/**
	 * Add a new peer to the snapshot.
	 * 
	 * @param address
	 *            the address of the peer
	 */
	public static void addPeer(Address address) {
		peers.put(address, new PeerInfo());
	}

	/**
	 * Remove a peer from the snapshot.
	 * 
	 * @param address
	 *            the address of the peer
	 */
	public static void removePeer(Address address) {
		peers.remove(address);
	}

	/**
	 * Increment the number of indexes for the given peer.
	 * 
	 * @param address
	 *            the address of the peer
	 */
	public static void incNumIndexEntries(Address address) {
		PeerInfo peerInfo = peers.get(address);

		if (peerInfo == null) {
			return;
		}

		peerInfo.incNumIndexEntries();
	}

	/**
	 * Set the neighbors in the snapshot of a peer.
	 * 
	 * @param address
	 *            the address of the peer
	 * @param partners
	 *            the neighbors to be set
	 */
	public static void updateNeighbours(Address address, ArrayList<Address> partners) {
		PeerInfo peerInfo = peers.get(address);

		if (peerInfo == null) {
			return;
		}

		peerInfo.setNeighbours(partners);
	}

	/**
	 * Set the leader status for a given peer.
	 * 
	 * @param address
	 *            the address of the peer
	 * @param leader
	 *            the leader status
	 */
	public static void setLeaderStatus(Address address, boolean leader) {
		PeerInfo peerInfo = peers.get(address);

		if (leader) {
			oldLeaders.add(address);
		}
		
		if (peerInfo == null) {
			return;
		}

		peerInfo.setLeader(leader);
	}

	/**
	 * Set the TMan view which was used when a node became the leader.s
	 * 
	 * @param address
	 *            the address of the peer
	 * @param view
	 *            the string representation of the TMan view
	 */
	public static void setElectionView(Address address, String view) {
		PeerInfo peerInfo = peers.get(address);

		if (peerInfo == null) {
			return;
		}

		peerInfo.setElectionView(view);
	}

	/**
	 * Add a string representation of the nodes TMan view to the snapshot.
	 * 
	 * @param address
	 *            the address of the peer
	 * @param view
	 *            the string representation of the TMan view
	 */
	public static void setCurrentView(Address address, String view) {
		PeerInfo peerInfo = peers.get(address);

		if (peerInfo == null) {
			return;
		}

		peerInfo.setCurrentView(view);
	}

	/**
	 * Add a detected gap to the snapshot.
	 * 
	 * @param gapNumber
	 *            the id of the gap
	 */
	public static void addGap(int gapNumber) {
		detectedGaps.add(gapNumber);
	}

	/**
	 * Add the last added index id to the snapshot.
	 * 
	 * @param id
	 *            the id of the lastest added index value
	 */
	public static synchronized void setLastId(int id) {
		if (id <= lastId) {
			idDuplicates.add(id);
		}
		lastId = id;

		entriesAdded++;
	}

	/**
	 * Create a report.
	 */
	public static void report() {
		StringBuilder builder = new StringBuilder();
		builder.append("current time: " + counter++ + "\n");
		reportNetworkState(builder);
		reportSmalestId(builder);
		reportLeaders(builder);
		reportOldLeaders(builder);
		reportDetails(builder);
		reportLastId(builder);
		reportIdDuplicates(builder);
		reportAmountOfGaps(builder);
		reportDetectedGaps(builder);
		builder.append("###\n");

		String str = builder.toString();
		System.out.println(str);
		FileIO.append(str, FILENAME);
	}

	/**
	 * Create the network report.
	 * 
	 * @param builder
	 *            the builder used to add the information
	 */
	private static void reportNetworkState(StringBuilder builder) {
		int totalNumOfPeers = peers.size();
		builder.append("total number of peers: ");
		builder.append(totalNumOfPeers);
		builder.append("\n");
	}

	/**
	 * Create a report of current leader..
	 * 
	 * @param builder
	 *            the builder used to add the information
	 */
	private static void reportLeaders(StringBuilder builder) {
		for (Address p : peers.keySet()) {
			PeerInfo info = peers.get(p);
			if (info.isLeader()) {
				builder.append(p.getId());
				builder.append(" is leader and its TMan view was: ");
				builder.append(info.getElectionView());
				builder.append("\n");
				builder.append("Its current view is: ");
				builder.append(info.getCurrentView());
				builder.append("\n");
			}
		}
	}

	/**
	 * Create a report about nodes with the minimal and maximal number of index
	 * entries stored.
	 * 
	 * @param builder
	 *            the builder used to add the information
	 */
	private static void reportDetails(StringBuilder builder) {
		Address maxPeer = null;
		Address minPeer = null;
		int maxNumIndexEntries = 0;
		int minNumIndexEntries = Integer.MAX_VALUE;
		for (Address node : peers.keySet()) {
			PeerInfo p = peers.get(node);
			if (p.getNumIndexEntries() < minNumIndexEntries) {
				minNumIndexEntries = p.getNumIndexEntries();
				minPeer = node;
			}
			if (p.getNumIndexEntries() > maxNumIndexEntries) {
				maxNumIndexEntries = p.getNumIndexEntries();
				maxPeer = node;
			}
		}
		builder.append(maxPeer == null ? "None" : maxPeer.getId());
		builder.append(" is the peer with max num of index entries: ");
		builder.append(maxNumIndexEntries);
		builder.append("\n");

		builder.append(minPeer == null ? "None" : minPeer.getId());
		builder.append(" is the peer with min num of index entries: ");
		builder.append(minNumIndexEntries);
		builder.append("\n");
	}

	/**
	 * Create a report about detected gaps.
	 * 
	 * @param builder
	 *            the builder used to add the information
	 */
	private static void reportDetectedGaps(StringBuilder builder) {
		builder.append("Detected index gaps: ");
		for (Integer number : detectedGaps) {
			builder.append(number);
			builder.append(" ");
		}
		builder.append("\n");
	}

	/**
	 * Create a report about the node with the smalest id.
	 * 
	 * @param builder
	 *            the builder used to add the information
	 */
	private static void reportSmalestId(StringBuilder builder) {
		builder.append("The smalest node id is: ");
		builder.append(peers.firstKey().getId());
		builder.append("\n");
	}

	/**
	 * Create a report about duplicated ids.
	 * 
	 * @param builder
	 *            the builder used to add the information
	 */
	private static void reportIdDuplicates(StringBuilder builder) {
		builder.append("Duplicated index ids: ");
		for (Integer number : idDuplicates) {
			builder.append(number);
			builder.append(" ");
		}
		builder.append("\n");
	}

	/**
	 * Create a report about the latest index id.
	 * 
	 * @param builder
	 *            the builder used to add the information
	 */
	private static void reportLastId(StringBuilder builder) {
		builder.append("Last index id: ");
		builder.append(lastId == -1 ? "" : lastId);
		builder.append("\n");
	}

	/**
	 * Create a report about the amount of gaps in the index range.
	 * 
	 * @param builder
	 *            the builder used to add the information
	 */
	private static void reportAmountOfGaps(StringBuilder builder) {
		builder.append("Amount of gaps: ");
		builder.append(lastId - entriesAdded + 1);
		builder.append("\n");
	}

	/**
	 * Create a report about all nodes that have been leader.s
	 * 
	 * @param builder
	 *            the builder used to add the information
	 */
	private static void reportOldLeaders(StringBuilder builder) {
		builder.append("Nodes that have been leader:\n");
		for (Address node : oldLeaders) {
			PeerInfo peer = peers.get(node);
			builder.append(node.getId());
			builder.append(" was leader with TMan view: ");
			if (peer != null) {
				builder.append(peer.getElectionView());
			} else {
				builder.append(" no view because the peer was removed");
			}
			
			builder.append("\n");
		}
	}
}
