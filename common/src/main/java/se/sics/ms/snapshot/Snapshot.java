package se.sics.ms.snapshot;

import se.sics.gvod.address.Address;
import se.sics.gvod.net.VodAddress;
import se.sics.ms.configuration.MsConfig;
import se.sics.ms.types.OverlayAddress;
import se.sics.ms.util.Pair;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Keep track of the system state for evaluation and debugging. Write the
 * information in a log file.
 */
public class Snapshot {
	private static SortedMap<OverlayAddress, PeerInfo> peers = Collections.synchronizedSortedMap(new TreeMap<OverlayAddress, PeerInfo>());
	private static int counter = 0;
	private static String FILENAME = "search.out";
    private static ConcurrentHashMap<Pair<Integer, Integer>, Long> maxIds = new ConcurrentHashMap<Pair<Integer, Integer>, Long>();
    private static ConcurrentHashMap<Pair<Integer, Integer>, Long> minIds = new ConcurrentHashMap<Pair<Integer, Integer>, Long>();
	private static ConcurrentSkipListSet<Long> idDuplicates = new ConcurrentSkipListSet<Long>();
    private static int receivedAddRequests = 0;
	private static int entriesAdded = 0;
    private static int failedAddRequests = 0;
	private static ConcurrentSkipListSet<VodAddress> oldLeaders = new ConcurrentSkipListSet<VodAddress>();

    private static long bestTime = Long.MAX_VALUE;
    private static long worstTime = Long.MIN_VALUE;
    private static ArrayList<Long> lastTimes = new ArrayList<Long>();

	public static void init(int numOfStripes) {
		FileIO.write("", FILENAME);
	}

	/**
	 * Add a new peer to the snapshot.
	 * 
	 * @param address
	 *            the address of the peer
	 */
	public static void addPeer(VodAddress address) {
		peers.put(new OverlayAddress(address), new PeerInfo());
	}

	/**
	 * Remove a peer from the snapshot.
	 * 
	 * @param address
	 *            the address of the peer
	 */
	public static void removePeer(VodAddress address) {
		peers.remove(new OverlayAddress(address));
	}

	/**
	 * Increment the number of indexes for the given peer.
	 * 
	 * @param address
	 *            the address of the peer
	 */
	public static void incNumIndexEntries(VodAddress address) {
		PeerInfo peerInfo = peers.get(new OverlayAddress(address));

		if (peerInfo == null) {
			return;
		}

		peerInfo.incNumIndexEntries();
	}

    public static void reportAddingTime(long time) {
        if(time < bestTime)
            bestTime = time;

        if(time > worstTime)
            worstTime = time;

        if(lastTimes.size() == 100)
            lastTimes.remove(lastTimes.get(0));
        lastTimes.add(time);
    }

    public static void setNumIndexEntries(VodAddress address, long value) {
        PeerInfo peerInfo = peers.get(new OverlayAddress(address));

        if (peerInfo == null) {
            return;
        }

        peerInfo.setNumIndexEntries(value);
    }

	/**
	 * Set the neighbors in the snapshot of a peer.
	 * 
	 * @param address
	 *            the address of the peer
	 * @param partners
	 *            the neighbors to be set
	 */
	public static void updateNeighbours(VodAddress address, ArrayList<Address> partners) {
		PeerInfo peerInfo = peers.get(new OverlayAddress(address));

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
	public static void setLeaderStatus(VodAddress address, boolean leader, LinkedList<Boolean> partition) {
		PeerInfo peerInfo = peers.get(new OverlayAddress(address));

		if (leader) {
			oldLeaders.add(address);
		}
		
		if (peerInfo == null) {
			return;
		}

        peerInfo.setPartitionId(partition);
		peerInfo.setLeader(leader);
	}

	/**
	 * Set the Gradient view which was used when a node became the leader.s
	 * 
	 * @param address
	 *            the address of the peer
	 * @param view
	 *            the string representation of the Gradient view
	 */
	public static void setElectionView(VodAddress address, String view) {
		PeerInfo peerInfo = peers.get(new OverlayAddress(address));

		if (peerInfo == null) {
			return;
		}

		peerInfo.setElectionView(view);
	}

	/**
	 * Add a string representation of the nodes Gradient view to the snapshot.
	 * 
	 * @param address
	 *            the address of the peer
	 * @param view
	 *            the string representation of the Gradient view
	 */
	public static void setCurrentView(VodAddress address, String view) {
		PeerInfo peerInfo = peers.get(new OverlayAddress(address));

		if (peerInfo == null) {
			return;
		}

		peerInfo.setCurrentView(view);
	}

	/**
	 * Add the last added index id to the snapshot.
	 *
     * @param categoryPartitionPair
     *            pair of node's category and partitionId
	 * @param id
	 *            the id of the lastest added index value
	 */
	public static synchronized void addIndexEntryId(Pair<Integer, Integer> categoryPartitionPair, long id) {
        entriesAdded++;

        Long lastId = maxIds.get(categoryPartitionPair);
        if (lastId == null) {
            lastId = id;
            maxIds.put(categoryPartitionPair, lastId);
            return;
        }

		if (id <= lastId.longValue() && id >= minIds.get(categoryPartitionPair).longValue()) {
			idDuplicates.add(id);
		}
		maxIds.put(categoryPartitionPair, id);
	}

    public static synchronized void resetPartitionHighestId(Pair<Integer, Integer> categoryPartitionPair, long id) {
        maxIds.put(categoryPartitionPair, id);
    }

    public static synchronized void resetPartitionLowestId(Pair<Integer, Integer> categoryPartitionPair, long id) {
        minIds.put(categoryPartitionPair, id);
    }

    public static synchronized void addPartition(Pair<Integer, Integer> categoryPartitionPair) {
        Long lastId = maxIds.get(categoryPartitionPair);
        if (lastId == null) {
            maxIds.put(categoryPartitionPair, Long.MIN_VALUE);
            minIds.put(categoryPartitionPair, 0L);
        }
    }

    public static synchronized void incrementReceivedAddRequests() {
        receivedAddRequests++;
    }

    public static synchronized void incrementFailedddRequests() {
        failedAddRequests++;
    }

	/**
	 * Create a report.
	 */
	public static void report() {

		StringBuilder builder = new StringBuilder();
		builder.append("current time: " + counter++ + "\n");
		reportNetworkState(builder);
        builder.append("\n");
		reportLowestId(builder);
        builder.append("\n");
		reportLeaders(builder);
        builder.append("\n");
		reportOldLeaders(builder);
        builder.append("\n");
		reportDetails(builder);
        builder.append("\n");
		reportLatestIds(builder);
        builder.append("\n");
        reportNumberOfEntries(builder);
        builder.append("\n");
        reportReceivedAddRequests(builder);
        builder.append("\n");
        reportFailedAddRequests(builder);
        builder.append("\n");
		reportIdDuplicates(builder);
        builder.append("\n");
        reportAddingBestAndWorstTime(builder);
		builder.append("---------------------------------------------------------------------------------------------\n");

		String str = builder.toString();
		System.out.println(str);
		FileIO.append(str, FILENAME);
	}

    private static void reportAddingBestAndWorstTime(StringBuilder builder) {
        if(worstTime == Long.MIN_VALUE)
            return;

        int lastLength = lastTimes.size();
        long sum=0;
        for(int i=0; i<lastLength; i++)
            sum+=lastTimes.get(i);

        builder.append(String.format("Adding index entry time. Average: %s ms; Best: %s ms; Worst: %s ms\n", sum/lastLength, bestTime, worstTime));
    }

    private static void reportFailedAddRequests(StringBuilder builder) {
        builder.append("Total number of failed AddRequests: " + failedAddRequests + "\n");
    }

    private static void reportReceivedAddRequests(StringBuilder builder) {
        builder.append("Total number of received AddRequests: " + receivedAddRequests + "\n");
    }

    private static void reportNumberOfEntries(StringBuilder builder) {
        builder.append("Total number of index values: " + entriesAdded + "\n");
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
		for (OverlayAddress p : peers.keySet()) {
			PeerInfo info = peers.get(p);
            if(info == null) continue;
			if (info.isLeader()) {
				builder.append(p.getId());
				builder.append(" is leader of partition ");
                builder.append(info.getPartitionId());
                builder.append(" for category \"");
                builder.append(MsConfig.Categories.values()[p.getCategoryId()]);
                builder.append("\"");
                builder.append("\n\tIts Gradient view was: ");
				builder.append(info.getElectionView());
				builder.append("\n\tIts current view is: ");
				builder.append(info.getCurrentView());
                builder.append("\n\tIts number of index entries: ");
                builder.append(info.getNumIndexEntries());
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
        VodAddress maxPeer = null;
        VodAddress minPeer = null;
		long maxNumIndexEntries = 0;
		long minNumIndexEntries = Integer.MAX_VALUE;
		for (OverlayAddress node : peers.keySet()) {
			PeerInfo p = peers.get(node);
            if(p == null) continue;
			if (p.getNumIndexEntries() < minNumIndexEntries) {
				minNumIndexEntries = p.getNumIndexEntries();
				//minPeer = node;
			}
			if (p.getNumIndexEntries() > maxNumIndexEntries) {
				maxNumIndexEntries = p.getNumIndexEntries();
				//maxPeer = node;
			}
		}
//		builder.append(maxPeer == null ? "None" : maxPeer.getId()-minPeer.getId()+1);
//		builder.append(" is the peer with max num of index entries: ");
//		builder.append(maxNumIndexEntries);
//		builder.append("\n");
//
//		builder.append(minPeer == null ? "None" : minPeer.getId());
//		builder.append(" is the peer with min num of index entries: ");
//		builder.append(minNumIndexEntries);
//		builder.append("\n");
	}

	/**
	 * Create a report about the node with the smalest id.
	 * 
	 * @param builder
	 *            the builder used to add the information
	 */
	private static void reportLowestId(StringBuilder builder) {
		builder.append("The lowest node id is: ");
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
        if(idDuplicates.isEmpty())
            return;

		builder.append("Duplicated index ids: ");
		for (Long number : idDuplicates) {
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
	private static void reportLatestIds(StringBuilder builder) {
		builder.append("Ids on partitions:\n");
        for (Pair<Integer, Integer> categoryPartitionPair : maxIds.keySet()) {
            long min = minIds.get(categoryPartitionPair) == null ? 0 : minIds.get(categoryPartitionPair);
            long max = maxIds.get(categoryPartitionPair) == null ? -1 : maxIds.get(categoryPartitionPair).longValue();
            builder.append(String.format("\t Category \"%s\". Partition %s. Min: %s Max: %s Total: %s\n",
                    MsConfig.Categories.values()[categoryPartitionPair.getFirst()], categoryPartitionPair.getSecond(),
                    min, max, Math.abs(max - min)));
        }
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
		for (VodAddress node : oldLeaders) {
			PeerInfo peer = peers.get(new OverlayAddress(node));
			builder.append("\t" + node.getId());
			builder.append(" was leader with gradient view: ");
			if (peer != null) {
				builder.append(peer.getElectionView());
			} else {
				builder.append(" no view because the peer was removed");
			}
			
			builder.append("\n");
		}
	}


    /**
     * Update the node information in the list.
     *
     * @param
     */
    public static void updateInfo(VodAddress address){

        OverlayAddress requiredOverlayAddress  = null;

        for(OverlayAddress peerAddress : peers.keySet() ){
            if(peerAddress.getId() == address.getId()){
                requiredOverlayAddress = peerAddress;
                break;
            }
        }

        if(requiredOverlayAddress == null)
            return;

        // Remove the existing entry from the map.
        PeerInfo requiredPeerInfo = peers.get(requiredOverlayAddress);
        peers.remove(requiredOverlayAddress);

        // add the updated entries in the map.
        peers.put(new OverlayAddress(address),requiredPeerInfo);

    }
}
