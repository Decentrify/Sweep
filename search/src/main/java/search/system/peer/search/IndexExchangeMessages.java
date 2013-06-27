package search.system.peer.search;

import java.util.List;

import se.sics.kompics.address.Address;
import se.sics.kompics.network.Message;

import common.entities.IndexEntry;

/**
 * Messages used to exchanges indexes between peers.
 */
public final class IndexExchangeMessages {
	private IndexExchangeMessages() {

	}

	/**
	 * Request for indexes higher than the given id.
	 */
	public static class IndexUpdateRequest extends Message {
		private static final long serialVersionUID = 563812527860881003L;
		private final long oldestMissingIndexValue;
		private final Long[] existingEntries;

		/**
		 * @param source
		 *            the message source
		 * @param destination
		 *            the message destination
		 * @param oldestMissingIndexValue
		 *            the oldest index id the inquirer is asking for
		 * @param existingEntries
		 *            an array of existing entries with greater ids than
		 *            oldestMissingIndexValue
		 */
		public IndexUpdateRequest(Address source, Address destination, long oldestMissingIndexValue,
				Long[] existingEntries) {
			super(source, destination);
			this.oldestMissingIndexValue = oldestMissingIndexValue;
			this.existingEntries = existingEntries;
		}

		/**
		 * @return the oldest index id the inquirer is asking for
		 */
		public long getOldestMissingIndexValue() {
			return oldestMissingIndexValue;
		}

		/**
		 * @return a set of existing entries with greater ids than
		 *         oldestMissingIndexValue
		 */
		public Long[] getExistingEntries() {
			return existingEntries;
		}
	}

	/**
	 * Response for an {@link IndexUpdateRequest} including the found entries.
	 */
	public static class IndexUpdateResponse extends Message {
		private static final long serialVersionUID = -8975735266476692815L;
		private final List<IndexEntry> indexEntries;

		/**
		 * @param source
		 *            the message source
		 * @param destination
		 *            the message destination
		 * @param indexEntries
		 *            the list of found {@link IndexEntry}s
		 */
		public IndexUpdateResponse(Address source, Address destination,
				List<IndexEntry> indexEntries) {
			super(source, destination);
			this.indexEntries = indexEntries;
		}

		/**
		 * @return the {@link IndexEntry}s found
		 */
		public List<IndexEntry> getIndexEntries() {
			return indexEntries;
		}
	}
}
