package search.system.peer.search;

import se.sics.kompics.address.Address;

/**
 * Data structure used by the leader to keep track of replication responses when
 * adding new entries.
 */
public class ReplicationCount {
	private final Address source;
	private int received;
	private final int replicationMinimum;

	/**
	 * @param source
	 *            the address of the node that issued the add request
	 * @param replicationMinimum
	 *            the minimum number of replication required
	 */
	public ReplicationCount(Address source, int replicationMinimum) {
		super();
		this.source = source;
		this.replicationMinimum = replicationMinimum;
	}

	/**
	 * @return the address of the node that issued the add request
	 */
	public Address getSource() {
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
}
