package se.sics.ms.search;

import se.sics.gvod.net.VodAddress;
import se.sics.peersearch.types.IndexEntry;

/**
 * Data structure used by the leader to keep track of replication responses when
 * adding new entries.
 */
public class ReplicationCount {
	private final VodAddress source;
	private int received;
	private final int replicationMinimum;
    private final IndexEntry entry;

	/**
     * @param source
     *            the address of the node that issued the add request
     * @param replicationMinimum
     * @param entry
     */
	public ReplicationCount(VodAddress source, int replicationMinimum, IndexEntry entry) {
		super();
		this.source = source;
		this.replicationMinimum = replicationMinimum;
        this.entry = entry;
    }

	/**
	 * @return the address of the node that issued the add request
	 */
	public VodAddress getSource() {
		return source;
	}

	/**
	 * @return the number of replication acknowledgments received
	 */
	public int getReceived() {
		return received;
	}

	/**
	 * @param received
	 *            the number of replication acknowledgments received
	 */
	public void setReceived(int received) {
		this.received = received;
	}

	/**
	 * Increment the replication count and check if the requirements were met.
	 * 
	 * @return true if the number of received acknowledgments reached the
	 *         replication requirements
	 */
	public boolean incrementAndCheckReceived() {
		received++;
		return received >= replicationMinimum;
	}

    public IndexEntry getEntry() {
        return entry;
    }
}
