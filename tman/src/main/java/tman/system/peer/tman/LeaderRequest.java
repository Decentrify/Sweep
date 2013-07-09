package tman.system.peer.tman;

import java.io.Serializable;
import java.util.UUID;

import se.sics.kompics.Event;
import se.sics.kompics.address.Address;
import se.sics.peersearch.types.IndexEntry;


/**
 * Superclass for requests sent to the leader.
 */
public abstract class LeaderRequest extends Event implements Serializable {
	private static final long serialVersionUID = -7670704362921626978L;
	private final Address source;
	private final UUID uuid;

	/**
	 * @param source
	 *            the message source
	 * @param uuid
	 *            the unique request id
	 */
	public LeaderRequest(Address source, UUID uuid) {
		super();
		this.source = source;
		this.uuid = uuid;
	}

	/**
	 * @return the message source
	 */
	public Address getSource() {
		return source;
	}

	/**
	 * @return the unique request id
	 */
	public UUID getUuid() {
		return uuid;
	}

	/**
	 * Request to add a new {@link IndexEntry}.
	 */
	public static class AddIndexEntry extends LeaderRequest {
		private static final long serialVersionUID = 1794144097056983843L;
		private final IndexEntry indexEntry;

		/**
		 * @param source
		 *            the message source
		 * @param uuid
		 *            the unique request id
		 * @param indexEntry
		 *            the {@link IndexEntry} to be added
		 */
		public AddIndexEntry(Address source, UUID uuid, IndexEntry indexEntry) {
			super(source, uuid);
			this.indexEntry = indexEntry;
		}

		/**
		 * @return the {@link IndexEntry} to be added
		 */
		public IndexEntry getIndexEntry() {
			return indexEntry;
		}
	}

	/**
	 * Request the leader to start the the gap detection progress for the given
	 * id.
	 */
	public static class GapCheck extends Event implements Serializable {
		private static final long serialVersionUID = 1952986693481364473L;
		private final long id;

		/**
		 * @param id
		 *            the id of the suspected entry
		 */
		public GapCheck(long id) {
			super();
			this.id = id;
		}

		/**
		 * @return the id of the suspected entry
		 */
		public long getId() {
			return id;
		}
	}
}
