package se.sics.ms.gradient;

import java.io.Serializable;

import se.sics.gvod.net.VodAddress;
import se.sics.gvod.timer.UUID;
import se.sics.kompics.Event;
import se.sics.peersearch.types.IndexEntry;


/**
 * Superclass for requests sent to the leader.
 */
public abstract class LeaderRequest extends Event implements Serializable {
	private static final long serialVersionUID = -7670704362921626978L;
	private final VodAddress source;
	private final UUID uuid;

	/**
	 * @param source
	 *            the message source
	 * @param uuid
	 *            the unique request id
	 */
	public LeaderRequest(VodAddress source, UUID uuid) {
		super();
		this.source = source;
		this.uuid = uuid;
	}

	/**
	 * @return the message source
	 */
	public VodAddress getSource() {
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
		public AddIndexEntry(VodAddress source, UUID uuid, IndexEntry indexEntry) {
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

        private final VodAddress source;
		private final long id;

		/**
         * @param source
         * @param id
         */
		public GapCheck(VodAddress source, long id) {
			super();
            this.source = source;
            this.id = id;
		}

		/**
		 * @return the id of the suspected entry
		 */
		public long getId() {
			return id;
		}

        public VodAddress getSource() {
            return source;
        }
    }
}