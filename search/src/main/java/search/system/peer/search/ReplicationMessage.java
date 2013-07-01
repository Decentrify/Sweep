package search.system.peer.search;

import java.util.UUID;

import se.sics.kompics.address.Address;
import se.sics.kompics.network.Message;

import se.sics.peersearch.data.types.IndexEntry;

/**
 * Superclass for messages used by the leader to store new entries on nodes.
 */
public abstract class ReplicationMessage extends Message {
	private static final long serialVersionUID = -637687429994590022L;
	private final UUID uuid;

	/**
	 * @param source
	 *            the source of the message
	 * @param destination
	 *            the destination of the message
	 * @param uuid
	 *            the unique request id
	 */
	protected ReplicationMessage(Address source, Address destination, UUID uuid) {
		super(source, destination);
		this.uuid = uuid;
	}

	/**
	 * @return the unique request id
	 */
	public UUID getUuid() {
		return uuid;
	}

	/**
	 * Message sent from the leader to other nodes to request them to store a
	 * new {@link# IndexEntry}.
	 */
	public static class Replicate extends ReplicationMessage {
		private static final long serialVersionUID = 9101545948009759686L;
		private final IndexEntry indexEntry;

		/**
		 * @param source
		 *            the source of the message
		 * @param destination
		 *            the destination of the message
		 * @param uuid
		 *            the unique request id
		 */
		protected Replicate(Address source, Address destination, IndexEntry indexEntry, UUID uuid) {
			super(source, destination, uuid);
			this.indexEntry = indexEntry;
		}

		/**
		 * @return the {@link IndexEntry} to be stored.
		 */
		public IndexEntry getIndexEntry() {
			return indexEntry;
		}
	}

	/**
	 * Acknowledgment for a {@link Replicate} request.
	 */
	public static class ReplicationConfirmation extends ReplicationMessage {
		private static final long serialVersionUID = -6300892774185908729L;

		/**
		 * @param source
		 *            the source of the message
		 * @param destination
		 *            the destination of the message
		 * @param uuid
		 *            the unique request id
		 */
		protected ReplicationConfirmation(Address source, Address destination, UUID uuid) {
			super(source, destination, uuid);
		}
	}
}
