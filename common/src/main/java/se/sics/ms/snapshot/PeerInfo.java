package se.sics.ms.snapshot;

import se.sics.gvod.address.Address;

import java.util.ArrayList;
import java.util.LinkedList;


/**
 * Stores information about a peer for snapshots.
 */
public class PeerInfo {
	private long numIndexEntries;
	private ArrayList<Address> neighbours;
	private boolean leader;
	private String electionView;
	private String currentView;
    private LinkedList<Boolean> partitionId;

	public PeerInfo() {
		this.neighbours = new ArrayList<Address>();
		this.numIndexEntries = 0;
		this.leader = false;
        this.partitionId = new LinkedList<Boolean>();
	}

	/**
	 * @return the number of index entries this peer has stored
	 */
	public synchronized long getNumIndexEntries() {
		return numIndexEntries;
	}

	/**
	 * Increment the number of index entries this peer has stored
	 */
	public synchronized void incNumIndexEntries() {
		this.numIndexEntries++;
	}

	/**
	 * Set the neighbors for this peer
	 * 
	 * @param partners
	 *            the neighbors to be set
	 */
	public synchronized void setNeighbours(ArrayList<Address> partners) {
		this.neighbours = partners;
	}

	/**
	 * @return the neighbors of this peer
	 */
	public synchronized ArrayList<Address> getNeighbours() {
		return new ArrayList<Address>(neighbours);
	}

	/**
	 * @return true if this peer is a leader
	 */
	public synchronized boolean isLeader() {
		return leader;
	}

	/**
	 * @param leader
	 *            the leader status
	 */
	public synchronized void setLeader(boolean leader) {
		this.leader = leader;
	}

	/**
	 * @return a string representation of the gradient view used to become a leader
	 */
	public synchronized String getElectionView() {
		return electionView;
	}

	/**
	 * @param gradientView
	 *            a string representation of the gradient view used to become a
	 *            leader
	 */
	public synchronized void setElectionView(String gradientView) {
		this.electionView = gradientView;
	}

	/**
	 * @return a string representation of the peers current gradient view
	 */
	public synchronized String getCurrentView() {
		return currentView;
	}

	/**
	 * @param currentView
	 *            a string representation of the peers current gradient view
	 */
	public synchronized void setCurrentView(String currentView) {
		this.currentView = currentView;
	}

    public synchronized LinkedList<Boolean> getPartitionId() {
        return partitionId;
    }

    public synchronized void setPartitionId(LinkedList<Boolean> partitionId) {
        this.partitionId = partitionId;
    }

    public void setNumIndexEntries(long numIndexEntries) {
        this.numIndexEntries = numIndexEntries;
    }
}
