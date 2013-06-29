package search.system.peer.search;

import java.util.Collection;
import java.util.UUID;

import se.sics.kompics.address.Address;
import se.sics.kompics.network.Message;

import common.entities.IndexEntry;

/**
 * Superclass for messages used in the search process.
 */
public abstract class SearchMessage extends Message {
	private static final long serialVersionUID = 3837930675284873716L;
	private final UUID requestId;

	/**
	 * @param source
	 *            the source of the message
	 * @param destination
	 *            the destination of the message
	 * @param uuid
	 *            the unique request id
	 */
	protected SearchMessage(Address source, Address destination, UUID requestId) {
		super(source, destination);
		this.requestId = requestId;
	}

	/**
	 * @return the unique request id
	 */
	public UUID getRequestId() {
		return requestId;
	}

	/**
	 * Search request sent to other nodes to request their entries that match
	 * the given search pattern.
	 */
	public static class SearchRequest extends SearchMessage {
		private static final long serialVersionUID = 7206588576049792634L;
		private final SearchPattern pattern;

		/**
		 * @param source
		 *            the source of the message
		 * @param destination
		 *            the destination of the message
		 * @param uuid
		 *            the unique request id
		 * @param pattern
		 *            the search pattern
		 */
		public SearchRequest(Address source, Address destination, UUID requestId,
				SearchPattern pattern) {
			super(source, destination, requestId);
			this.pattern = pattern;
		}

		/**
		 * @return the search pattern
		 */
		public SearchPattern getSearchPattern() {
			return pattern;
		}
	}

	/**
	 * Answer to a {@link SearchRequest} including the matching results.
	 */
	public static class SearchResponse extends SearchMessage {
		private static final long serialVersionUID = 9150491478660241550L;
		private final Collection<IndexEntry> results;

		/**
		 * @param source
		 *            the source of the message
		 * @param destination
		 *            the destination of the message
		 * @param uuid
		 *            the unique request id
		 * @param results
		 *            the list of the {@link IndexEntry} entries matching the
		 *            request query.
		 */
		protected SearchResponse(Address source, Address destination, UUID requestId,
				Collection<IndexEntry> results) {
			super(source, destination, requestId);
			this.results = results;
		}

		/**
		 * @return the list of the {@link IndexEntry} entries matching the
		 *         request query.
		 */
		public Collection<IndexEntry> getResults() {
			return results;
		}
	}
}
